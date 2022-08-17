/*
    Copyright (c) 2007-2016 Contributors as noted in the AUTHORS file

    This file is part of libzmq, the ZeroMQ core engine in C++.

    libzmq is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License (LGPL) as published
    by the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    As a special exception, the Contributors give you permission to link
    this library with independent modules to produce an executable,
    regardless of the license terms of these independent modules, and to
    copy and distribute the resulting executable under terms of your choice,
    provided that you also meet, for each linked independent module, the
    terms and conditions of the license of that module. An independent
    module is a module which is not derived from or based on this library.
    If you modify this library, you must extend this exception to your
    version of the library.

    libzmq is distributed in the hope that it will be useful, but WITHOUT
    ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
    FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
    License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "precompiled.hpp"
#include "macros.hpp"

#include <limits.h>
#include <string.h>

#ifndef ZMQ_HAVE_WINDOWS
#    include <unistd.h>
#endif

#include <new>
#include <sstream>

#include "stream_engine_base.hpp"
#include "io_thread.hpp"
#include "session_base.hpp"
#include "v1_encoder.hpp"
#include "v1_decoder.hpp"
#include "v2_encoder.hpp"
#include "v2_decoder.hpp"
#include "null_mechanism.hpp"
#include "plain_client.hpp"
#include "plain_server.hpp"
#include "gssapi_client.hpp"
#include "gssapi_server.hpp"
#include "curve_client.hpp"
#include "curve_server.hpp"
#include "raw_decoder.hpp"
#include "raw_encoder.hpp"
#include "config.hpp"
#include "err.hpp"
#include "ip.hpp"
#include "tcp.hpp"
#include "likely.hpp"
#include "wire.hpp"

static std::string get_peer_address(zmq::fd_t s_)
{
    std::string peer_address;

    const int family = zmq::get_peer_ip_address(s_, peer_address);
    if (family == 0)
        peer_address.clear();
#if defined ZMQ_HAVE_SO_PEERCRED
    else if (family == PF_UNIX)
    {
        struct ucred cred;
        socklen_t size = sizeof(cred);
        if (!getsockopt(s_, SOL_SOCKET, SO_PEERCRED, &cred, &size))
        {
            std::ostringstream buf;
            buf << ":" << cred.uid << ":" << cred.gid << ":" << cred.pid;
            peer_address += buf.str();
        }
    }
#elif defined ZMQ_HAVE_LOCAL_PEERCRED
    else if (family == PF_UNIX)
    {
        struct xucred cred;
        socklen_t size = sizeof(cred);
        if (!getsockopt(s_, 0, LOCAL_PEERCRED, &cred, &size) && cred.cr_version == XUCRED_VERSION)
        {
            std::ostringstream buf;
            buf << ":" << cred.cr_uid << ":";
            if (cred.cr_ngroups > 0)
                buf << cred.cr_groups[0];
            buf << ":";
            peer_address += buf.str();
        }
    }
#endif

    return peer_address;
}

zmq::stream_engine_base_t::stream_engine_base_t(
    fd_t fd_, const options_t &options_, const endpoint_uri_pair_t &endpoint_uri_pair_, bool has_handshake_stage_)
    : _options(options_)
    , _inpos(NULL)
    , _insize(0)
    , _decoder(NULL)
    , _outpos(NULL)
    , _outsize(0)
    , _encoder(NULL)
    , _mechanism(NULL)
    , _next_msg(NULL)
    , _process_msg(NULL)
    , _metadata(NULL)
    , _input_stopped(false)
    , _output_stopped(false)
    , _endpoint_uri_pair(endpoint_uri_pair_)
    , _has_handshake_timer(false)
    , _has_ttl_timer(false)
    , _has_timeout_timer(false)
    , _has_heartbeat_timer(false)
    , _peer_address(get_peer_address(fd_))
    , _s(fd_)
    , _handle(static_cast<handle_t>(NULL))
    , _plugged(false)
    , _handshaking(true)
    , _io_error(false)
    , _session(NULL)
    , _socket(NULL)
    , _has_handshake_stage(has_handshake_stage_)
{
    const int rc = _tx_msg.init();
    errno_assert(rc == 0);

    //  Put the socket into non-blocking mode.
    unblock_socket(_s);
}

zmq::stream_engine_base_t::~stream_engine_base_t()
{
    zmq_assert(!_plugged);

    if (_s != retired_fd)
    {
#ifdef ZMQ_HAVE_WINDOWS
        const int rc = closesocket(_s);
        wsa_assert(rc != SOCKET_ERROR);
#else
        int rc = close(_s);
#    if defined(__FreeBSD_kernel__) || defined(__FreeBSD__)
        // FreeBSD may return ECONNRESET on close() under load but this is not
        // an error.
        if (rc == -1 && errno == ECONNRESET)
            rc = 0;
#    endif
        errno_assert(rc == 0);
#endif
        _s = retired_fd;
    }

    const int rc = _tx_msg.close();
    errno_assert(rc == 0);

    //  Drop reference to metadata and destroy it if we are
    //  the only user.
    if (_metadata != NULL)
    {
        if (_metadata->drop_ref())
        {
            LIBZMQ_DELETE(_metadata);
        }
    }

    LIBZMQ_DELETE(_encoder);
    LIBZMQ_DELETE(_decoder);
    LIBZMQ_DELETE(_mechanism);
}

// 将 conn_fd 加入到内核监听队列
void zmq::stream_engine_base_t::plug(io_thread_t *io_thread_, session_base_t *session_)
{
    zmq_assert(!_plugged);
    _plugged = true;

    //  Connect to session object.
    zmq_assert(!_session);
    zmq_assert(session_);
    _session = session_; // session 绑定到 engine
    _socket = _session->get_socket();

    //  Connect to I/O threads poller object.
    io_object_t::plug(io_thread_); // engine 获取 io_thread 的 poller. engine 和 session 使用了同一个线程的 poller

    // 将 conn_fd 加入到 poller 的内核监听队列中(在派生类的 plug_internal 中会设置 in_event 和 out_event)
    _handle = add_fd(_s);
    _io_error = false;

    plug_internal();
}

void zmq::stream_engine_base_t::unplug()
{
    zmq_assert(_plugged);
    _plugged = false;

    //  Cancel all timers.
    if (_has_handshake_timer)
    {
        cancel_timer(handshake_timer_id);
        _has_handshake_timer = false;
    }

    if (_has_ttl_timer)
    {
        cancel_timer(heartbeat_ttl_timer_id);
        _has_ttl_timer = false;
    }

    if (_has_timeout_timer)
    {
        cancel_timer(heartbeat_timeout_timer_id);
        _has_timeout_timer = false;
    }

    if (_has_heartbeat_timer)
    {
        cancel_timer(heartbeat_ivl_timer_id);
        _has_heartbeat_timer = false;
    }
    //  Cancel all fd subscriptions.
    if (!_io_error)
        rm_fd(_handle);

    //  Disconnect from I/O threads poller object.
    io_object_t::unplug();

    _session = NULL;
}

void zmq::stream_engine_base_t::terminate()
{
    unplug();
    delete this;
}

/***
 * in_event 接口被调用到的两种情况：
 * 1. 监听 conn_fd 被触发，从 epoll 返回，调用到 in_event()
 * 2. 在 zmtp_engine_t::plug_internal()/raw_engine_t::plug_internal 中直接调用。调用到 plug_internal() 表明已经完成了连接。所以在这里直接调用
 * in_event() 尝试从 socket 中读取数据，并不一定要等到 epoll 返回才可以
 */
void zmq::stream_engine_base_t::in_event()
{
    // ignore errors
    const bool res = in_event_internal();
    LIBZMQ_UNUSED(res);
}

/**
 * ZMTP 协议是 ZeroMQ 默认的消息传输协议，在 TCP
 * 协议之上定义了向后兼容性的规则，可扩展的安全机制，命令和消息分帧，连接元数据，以及其他传输层功能。所有有关 handshake 的都是 ZMTP 协议的处理逻辑。
 *
 * 如果用户需要使用裸 TCP 协议，则可以创建 raw_engine_t
 */
bool zmq::stream_engine_base_t::in_event_internal()
{
    zmq_assert(!_io_error);

    //  If still handshaking, receive and process the greeting message.
    if (unlikely(_handshaking)) // 在构造函数中被初始化成了 true
    {
        /**
         * 1. zmtp_engine: handshake() 会从 conn_fd 中读取对端发送的 greeting 消息，并修改 _process_msg 和 _next_msg
         * 指针的指向（这很重要，发送和接收routing_id_msg 都是在 _process_msg 和 _next_msg 中完成的）
         *
         * 2. raw_engine: handshake() 直接返回 true
         */
        if (handshake())
        {
            //  Handshaking was successful.
            //  Switch into the normal message flow.
            _handshaking = false;

            if (_mechanism == NULL && _has_handshake_stage)
                _session->engine_ready();
        }
        // greeting 的过程需要双方收发两条消息，所以只有第二条 greeting 完全接收完之后，handshake() 才会返回 true，其他情况都返回 false
        else
            return false;
    }

    /****************handshake() 返回 true 之后才会执行到这里**************/

    zmq_assert(_decoder);

    //  If there has been an I/O error, stop polling.
    if (_input_stopped)
    {
        rm_fd(_handle);
        _io_error = true;
        return true; // TODO or return false in this case too?
    }

    //  If there's no data to process in the buffer...
    if (!_insize)
    {
        //  Retrieve the buffer and read as much data as possible.
        //  Note that buffer can be arbitrarily large. However, we assume
        //  the underlying TCP layer has fixed buffer size and thus the
        //  number of bytes read will be always limited.
        size_t bufsize = 0;
        // 在这里分配了 buffer 的内存，并让 _inpos 指向这块 buffer
        _decoder->get_buffer(&_inpos, &bufsize);

        const int rc = read(_inpos, bufsize); // engine 从内核读取数据

        if (rc == -1)
        {
            if (errno != EAGAIN)
            {
                // 如果 socket 读错误，则会执行 terminate 流程.
                // socket 读错误出现之前可能已经在 out_event 中出现了 socket 写错误，但是在 out_event 中并不会执行 terminate
                error(connection_error);
                return false;
            }
            return true; // 如果读取不到数据，则返回 true（连接 socket 无数据可读）
        }

        //  Adjust input size
        _insize = static_cast<size_t>(rc);
        // Adjust buffer size to received bytes
        _decoder->resize_buffer(_insize);
    }

    int rc = 0;
    size_t processed = 0;

    while (_insize > 0)
    {
        rc = _decoder->decode(_inpos, _insize, processed);
        zmq_assert(processed <= _insize);
        _inpos += processed;
        _insize -= processed;
        if (rc == 0 || rc == -1)
            break;

        /**
         * 在 zmtp_engine 的构造函数中，_process_msg 指向了 process_routing_id_msg，但是在 handshake 中将 _process_msg 指向 process_handshake_command
         * 在 process_handshake_command 中，接收对端发送过来的 routing_id 消息，然后将该 routing_id 消息递交给 socket, 由 socket 生成 UUID
         */
        rc = (this->*_process_msg)(_decoder->msg());
        if (rc == -1)
            break;
    }

    //  Tear down the connection if we have failed to decode input data
    //  or the session has rejected the message.
    if (rc == -1)
    {
        if (errno != EAGAIN)
        {
            error(protocol_error);
            return false;
        }
        // 如果 errno == EAGAIN，说明缓冲区已经被写满，需要停止写入，从内核监听队列删除 EPOLLIN 事件，不再监听 EPOLLIN 事件
        _input_stopped = true;
        reset_pollin(_handle);
    }

    _session->flush(); // 刷新数据
    return true;
}

/**
 * EPOLLOUT 触发时间：
 * 1. TCP 连接建立完毕触发一次
 * 2. 缓冲区由不可写转变为可写的时候触发一次
 */

/**
 * 会有两个地方触发 out_event()
 * 1. conn_fd 可写，会从 epoll 中直接执行 out_event()。如果没有数据可写，会将 _output_stopped 置为 true
 * 2. conn_fd 可读，epoll 调用到 in_event(). process_handshake_command()--->restart_output()--->out_event()
 * 然后判断如果 _output_stopped 为 true，则会调用到 out_event()
 *
 * 注意 epoll 中是先执行 out_event()，然后执行 in_event()
 *
 * 也就是说，当 server-client 双方建立连接之后，server 会向 socket 写入 greeting 消息。greeting 消息在 zmtp_engine_t::plug_internal() 中构造的
 *
 * 同样的，client 端在监听到 conn_fd 可写之后，也会向 server 端写入 greeting 消息
 */
void zmq::stream_engine_base_t::out_event()
{
    zmq_assert(!_io_error);

    /**
     * 1. 当连接建立完成之后，EPOLLOUT 事件触发，此时由于 _outpos 中填充了 greeting 消息，_outsize 非 0，
     * 所以本端会直接向对端发送 outpos 中的 greeting 消息，同样的，对端也会是这样的操作。然后，双方在 in_event 中分别解析对端的 greeting 消息
     *
     * 2. 当 handshake 完成之后，outsize 为 0，（req/dealer/router 类型的 socket）在 next_handshake_command
     * 中开始构造 routing_id 消息，发送给对端，由对端的 socket 生成 UUID
     *
     * 3. 上述两步全部完成之后，才开始发送正常的消息
     */

    //  If write buffer is empty, try to read new data from the encoder.
    if (!_outsize)
    {
        //  Even when we stop polling as soon as there is no
        //  data to send, the poller may invoke out_event one
        //  more time due to 'speculative write' optimisation.
        if (unlikely(_encoder == NULL))
        {
            zmq_assert(_handshaking);
            return;
        }

        _outpos = NULL;
        _outsize = _encoder->encode(&_outpos, 0); // 这里将 _outpos 指向了一块分配好的内存，该内存块默认大小为 8192

        /**
         * while 循环在做的事情：
         * 1. 从 socket 和 session 通信的队列中读取消息（socket 可能发送了多帧消息，这多帧消息存放在不同的内存，每次 pull
         * 的时候拿到的是每一帧消息的指针）
         * 2. 拿到每一帧存放在不同内存区域的消息的指针，将其拷贝到连续的一块内存区域 _outpos 中
         */
        while (_outsize < static_cast<size_t>(_options.out_batch_size))
        {
            /**
             * 在 zmtp_engine 的构造函数中，_next_msg 是指向 routing_id_msg 的，但是在 handshake() 中，将 _next_msg 指向了 next_handshake_command
             * 在 next_handshake_command 调用中会构造一条 routing_id 的消息，然后发送给对端 socket，由对端 socket 生成 UUID
             */
            if ((this->*_next_msg)(&_tx_msg) == -1)
            {
                //  ws_engine can cause an engine error and delete it, so
                //  bail out immediately to avoid use-after-free
                if (errno == ECONNRESET)
                    return;
                else
                    break;
            }
            _encoder->load_msg(&_tx_msg);
            unsigned char *bufptr = _outpos + _outsize;
            const size_t n = _encoder->encode(&bufptr, _options.out_batch_size - _outsize);
            zmq_assert(n > 0);
            if (_outpos == NULL)
                _outpos = bufptr;
            _outsize += n;
        }

        //  If there is no data to send, stop polling for output.
        if (_outsize == 0)
        {
            _output_stopped = true; // 没有数据可以发送，不需要监听 EPOLLOUT 事件，但是何时开始重新监听 EPOLLOUT 事件的？？？ ---> restart_output
            reset_pollout();
            return;
        }
    }

    //  If there are any data to write in write buffer, write as much as
    //  possible to the socket. Note that amount of data to write can be
    //  arbitrarily large. However, we assume that underlying TCP layer has
    //  limited transmission buffer and thus the actual number of bytes
    //  written should be reasonably modest.
    const int nbytes = write(_outpos, _outsize); // engine 将数据写给 内核（_outpos 是一块连续的内存区域，包含要发送的消息，可能是多帧）

    //  IO error has occurred. We stop waiting for output events.
    //  The engine is not terminated until we detect input error;
    //  this is necessary to prevent losing incoming messages.
    // 为了防止输入消息丢失, socket 写错误的时候不会 terminate engine, 只有在 socket 读错误的时候才会执行 terminate.
    if (nbytes == -1)
    {
        // IO 发生错误（socket 错误或者发送缓冲区已写满），停止监听 EPOLLOUT 事件（下次发送缓冲区可写的时候可以重新监听 EPOLLOUT 事件）
        reset_pollout();
        return;
    }

    _outpos += nbytes;
    _outsize -= nbytes; // 数据发送完毕后，_outsize 会被重置为 0

    //  If we are still handshaking and there are no data
    //  to send, stop polling for output.
    if (unlikely(_handshaking))
        if (_outsize == 0)
            reset_pollout(); // 只有当仍在 handshanke 并且没有数据发送的时候，才不会继续监听 EPOLLOUT 事件
}

/**
 * 重新监听 EPOLLOUT 事件。
 *
 * 上一次 EPOLLOUT 事件触发之后，session 会从和 socket 通信的管道中读取 socket 写入的数据并交给 engine 将数据发送给内核，
 * 数据发送完毕之后，会从内核的监听队列中清除 EPOLLOUT 事件。
 *
 * 当 socket 重新向管道中写入了数据（如果是多帧的话，需要写入最后一帧才算完全写入），会 flush 消息，该 flush 动作实际上是给 session 发送
 * 消息，session 可以从管道中读取数据并交给 engine 写入给内核，并重新监听 EPOLLOUT 事件
 */
void zmq::stream_engine_base_t::restart_output()
{
    if (unlikely(_io_error))
        return;
    // 重新监听 EPOLLOUT 事件，并尝试重新发送数据（数据发送完毕之后，会将 _output_stopped 置为 true，并且停止监听 EPOLLOUT 事件）
    if (likely(_output_stopped))
    {
        set_pollout();
        _output_stopped = false;
    }

    //  Speculative write: The assumption is that at the moment new message
    //  was sent by the user the socket is probably available for writing.
    //  Thus we try to write the data to socket avoiding polling for POLLOUT.
    //  Consequently, the latency should be better in request/reply scenarios.
    out_event(); // 直接写，不用等 EPOLLOUT 触发
}

bool zmq::stream_engine_base_t::restart_input()
{
    zmq_assert(_input_stopped);
    zmq_assert(_session != NULL);
    zmq_assert(_decoder != NULL);

    int rc = (this->*_process_msg)(_decoder->msg());
    if (rc == -1)
    {
        if (errno == EAGAIN)
            _session->flush();
        else
        {
            error(protocol_error);
            return false;
        }
        return true;
    }

    while (_insize > 0)
    {
        size_t processed = 0;
        rc = _decoder->decode(_inpos, _insize, processed);
        zmq_assert(processed <= _insize);
        _inpos += processed;
        _insize -= processed;
        if (rc == 0 || rc == -1)
            break;
        rc = (this->*_process_msg)(_decoder->msg());
        if (rc == -1)
            break;
    }

    if (rc == -1 && errno == EAGAIN)
        _session->flush();
    else if (_io_error)
    {
        error(connection_error);
        return false;
    }
    else if (rc == -1)
    {
        error(protocol_error);
        return false;
    }

    else
    {
        _input_stopped = false;
        set_pollin();
        _session->flush();

        //  Speculative read.
        if (!in_event_internal())
            return false;
    }

    return true;
}

int zmq::stream_engine_base_t::next_handshake_command(msg_t *msg_)
{
    zmq_assert(_mechanism != NULL);

    if (_mechanism->status() == mechanism_t::ready)
    {
        mechanism_ready();
        return pull_and_encode(msg_);
    }
    if (_mechanism->status() == mechanism_t::error)
    {
        errno = EPROTO;
        return -1;
    }
    const int rc = _mechanism->next_handshake_command(msg_); // 内部会构造 routing_id 的消息

    if (rc == 0)
        msg_->set_flags(msg_t::command);

    return rc;
}

int zmq::stream_engine_base_t::process_handshake_command(msg_t *msg_)
{
    zmq_assert(_mechanism != NULL);
    const int rc = _mechanism->process_handshake_command(msg_); // 接收对端发送过来的 routing_id 消息，并保存到本地
    if (rc == 0)
    {
        if (_mechanism->status() == mechanism_t::ready)
            mechanism_ready(); // 创建 session 和 socket 通信的 pipe, 并将接收到的 routing_id 消息发送给 socket
        else if (_mechanism->status() == mechanism_t::error)
        {
            errno = EPROTO;
            return -1;
        }
        if (_output_stopped)
            restart_output();
    }

    return rc;
}

void zmq::stream_engine_base_t::zap_msg_available()
{
    zmq_assert(_mechanism != NULL);

    const int rc = _mechanism->zap_msg_available();
    if (rc == -1)
    {
        error(protocol_error);
        return;
    }
    if (_input_stopped)
        if (!restart_input())
            return;
    if (_output_stopped)
        restart_output();
}

const zmq::endpoint_uri_pair_t &zmq::stream_engine_base_t::get_endpoint() const
{
    return _endpoint_uri_pair;
}

void zmq::stream_engine_base_t::mechanism_ready()
{
    if (_options.heartbeat_interval > 0 && !_has_heartbeat_timer)
    {
        add_timer(_options.heartbeat_interval, heartbeat_ivl_timer_id);
        _has_heartbeat_timer = true;
    }

    if (_has_handshake_stage)
        _session->engine_ready(); // 创建 session 和 socket 通信的 pipe，并给 socket 发送命令绑定 pipe, 后面 socket 可以通过该 pipe 读取消息

    bool flush_session = false;

    if (_options.recv_routing_id)
    {
        msg_t routing_id;
        _mechanism->peer_routing_id(&routing_id); // 拷贝接收到的 routing_id 消息
        const int rc = _session->push_msg(&routing_id); // 将 routing_id 消息发送给 session（其实是写入到上面创建的和 socket 通信的 pipe 中）
        if (rc == -1 && errno == EAGAIN)
        {
            // If the write is failing at this stage with
            // an EAGAIN the pipe must be being shut down,
            // so we can just bail out of the routing id set.
            return;
        }
        errno_assert(rc == 0);
        flush_session = true;
    }

    if (_options.router_notify & ZMQ_NOTIFY_CONNECT)
    {
        msg_t connect_notification;
        connect_notification.init();
        const int rc = _session->push_msg(&connect_notification);
        if (rc == -1 && errno == EAGAIN)
        {
            // If the write is failing at this stage with
            // an EAGAIN the pipe must be being shut down,
            // so we can just bail out of the notification.
            return;
        }
        errno_assert(rc == 0);
        flush_session = true;
    }

    if (flush_session)
        _session->flush(); // 给 socket 发送信号，从 session 中接收消息

    // 收到 routing_id 消息之后， 改变 _next_msg 和 _process_msg 函数指针，从此之后，开始正常的发送和接收消息流程
    _next_msg = &stream_engine_base_t::pull_and_encode;
    _process_msg = &stream_engine_base_t::write_credential;

    //  Compile metadata.
    properties_t properties;
    init_properties(properties);

    //  Add ZAP properties.
    const properties_t &zap_properties = _mechanism->get_zap_properties();
    properties.insert(zap_properties.begin(), zap_properties.end());

    //  Add ZMTP properties.
    const properties_t &zmtp_properties = _mechanism->get_zmtp_properties();
    properties.insert(zmtp_properties.begin(), zmtp_properties.end());

    zmq_assert(_metadata == NULL);
    if (!properties.empty())
    {
        _metadata = new (std::nothrow) metadata_t(properties);
        alloc_assert(_metadata);
    }

    if (_has_handshake_timer)
    {
        cancel_timer(handshake_timer_id);
        _has_handshake_timer = false;
    }

    _socket->event_handshake_succeeded(_endpoint_uri_pair, 0);
}

int zmq::stream_engine_base_t::write_credential(msg_t *msg_)
{
    zmq_assert(_mechanism != NULL);
    zmq_assert(_session != NULL);

    const blob_t &credential = _mechanism->get_user_id();
    if (credential.size() > 0)
    {
        msg_t msg;
        int rc = msg.init_size(credential.size());
        zmq_assert(rc == 0);
        memcpy(msg.data(), credential.data(), credential.size());
        msg.set_flags(msg_t::credential);
        rc = _session->push_msg(&msg);
        if (rc == -1)
        {
            rc = msg.close();
            errno_assert(rc == 0);
            return -1;
        }
    }
    _process_msg = &stream_engine_base_t::decode_and_push; // 在这里，将从内核读取出来的用户数据 decode 之后，然后 push 给 session
    return decode_and_push(msg_);
}

int zmq::stream_engine_base_t::pull_and_encode(msg_t *msg_)
{
    zmq_assert(_mechanism != NULL);

    if (_session->pull_msg(msg_) == -1)
        return -1;
    if (_mechanism->encode(msg_) == -1)
        return -1;
    return 0;
}

int zmq::stream_engine_base_t::decode_and_push(msg_t *msg_)
{
    zmq_assert(_mechanism != NULL);

    if (_mechanism->decode(msg_) == -1)
        return -1;

    if (_has_timeout_timer)
    {
        _has_timeout_timer = false;
        cancel_timer(heartbeat_timeout_timer_id);
    }

    if (_has_ttl_timer)
    {
        _has_ttl_timer = false;
        cancel_timer(heartbeat_ttl_timer_id);
    }

    if (msg_->flags() & msg_t::command)
    {
        process_command_message(msg_);
    }

    if (_metadata)
        msg_->set_metadata(_metadata);
    if (_session->push_msg(msg_) == -1)
    {
        if (errno == EAGAIN) // 如果 socket 和 session 通信的 pipe 已经被写满，则会在这里修改 _process_msg 的指针
            _process_msg = &stream_engine_base_t::push_one_then_decode_and_push;
        return -1;
    }
    return 0;
}

int zmq::stream_engine_base_t::push_one_then_decode_and_push(msg_t *msg_)
{
    const int rc = _session->push_msg(msg_);
    if (rc == 0)
        _process_msg = &stream_engine_base_t::decode_and_push;
    return rc;
}

int zmq::stream_engine_base_t::pull_msg_from_session(msg_t *msg_)
{
    return _session->pull_msg(msg_);
}

int zmq::stream_engine_base_t::push_msg_to_session(msg_t *msg_)
{
    return _session->push_msg(msg_);
}

void zmq::stream_engine_base_t::error(error_reason_t reason_)
{
    zmq_assert(_session);

    if ((_options.router_notify & ZMQ_NOTIFY_DISCONNECT) && !_handshaking)
    {
        // For router sockets with disconnect notification, rollback
        // any incomplete message in the pipe, and push the disconnect
        // notification message.
        _session->rollback();

        msg_t disconnect_notification;
        disconnect_notification.init();
        _session->push_msg(&disconnect_notification);
    }

    // protocol errors have been signaled already at the point where they occurred
    if (reason_ != protocol_error && (_mechanism == NULL || _mechanism->status() == mechanism_t::handshaking))
    {
        const int err = errno;
        _socket->event_handshake_failed_no_detail(_endpoint_uri_pair, err);
        // special case: connecting to non-ZMTP process which immediately drops connection,
        // or which never responds with greeting, should be treated as a protocol error
        // (i.e. stop reconnect)
        if (((reason_ == connection_error) || (reason_ == timeout_error)) && (_options.reconnect_stop & ZMQ_RECONNECT_STOP_HANDSHAKE_FAILED))
        {
            reason_ = protocol_error;
        }
    }

    _socket->event_disconnected(_endpoint_uri_pair, _s);
    _session->flush();
    _session->engine_error(!_handshaking && (_mechanism == NULL || _mechanism->status() != mechanism_t::handshaking), reason_);
    unplug();
    delete this;   // 由于 engine 并没有托管到对象树 own 中，所以这里需要手动删除 engine. session 会在整个进程退出的时候在对象树中执行析构操作
}

void zmq::stream_engine_base_t::set_handshake_timer()
{
    zmq_assert(!_has_handshake_timer);

    if (_options.handshake_ivl > 0)
    {
        add_timer(_options.handshake_ivl, handshake_timer_id);
        _has_handshake_timer = true;
    }
}

bool zmq::stream_engine_base_t::init_properties(properties_t &properties_)
{
    if (_peer_address.empty())
        return false;
    properties_.ZMQ_MAP_INSERT_OR_EMPLACE(std::string(ZMQ_MSG_PROPERTY_PEER_ADDRESS), _peer_address);

    //  Private property to support deprecated SRCFD
    std::ostringstream stream;
    stream << static_cast<int>(_s);
    std::string fd_string = stream.str();
    properties_.ZMQ_MAP_INSERT_OR_EMPLACE(std::string("__fd"), ZMQ_MOVE(fd_string));
    return true;
}

void zmq::stream_engine_base_t::timer_event(int id_)
{
    if (id_ == handshake_timer_id)
    {
        _has_handshake_timer = false;
        //  handshake timer expired before handshake completed, so engine fail
        error(timeout_error);
    }
    else if (id_ == heartbeat_ivl_timer_id)
    {
        _next_msg = &stream_engine_base_t::produce_ping_message;
        out_event();
        add_timer(_options.heartbeat_interval, heartbeat_ivl_timer_id);
    }
    else if (id_ == heartbeat_ttl_timer_id)
    {
        _has_ttl_timer = false;
        error(timeout_error);
    }
    else if (id_ == heartbeat_timeout_timer_id)
    {
        _has_timeout_timer = false;
        error(timeout_error);
    }
    else
        // There are no other valid timer ids!
        assert(false);
}

int zmq::stream_engine_base_t::read(void *data_, size_t size_)
{
    const int rc = zmq::tcp_read(_s, data_, size_);

    if (rc == 0)
    {
        // connection closed by peer
        errno = EPIPE;
        return -1;
    }

    return rc;
}

int zmq::stream_engine_base_t::write(const void *data_, size_t size_)
{
    return zmq::tcp_write(_s, data_, size_);
}
