/*
    Copyright (c) 2007-2019 Contributors as noted in the AUTHORS file

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

#include "zmtp_engine.hpp"
#include "io_thread.hpp"
#include "session_base.hpp"
#include "v1_encoder.hpp"
#include "v1_decoder.hpp"
#include "v2_encoder.hpp"
#include "v2_decoder.hpp"
#include "v3_1_encoder.hpp"
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
#include "likely.hpp"
#include "wire.hpp"

zmq::zmtp_engine_t::zmtp_engine_t(fd_t fd_, const options_t &options_, const endpoint_uri_pair_t &endpoint_uri_pair_)
    : stream_engine_base_t(fd_, options_, endpoint_uri_pair_, true)
    , _greeting_size(v2_greeting_size)
    , _greeting_bytes_read(0)
    , _subscription_required(false)
    , _heartbeat_timeout(0)
{
    // 如果创建的 zmtp_engine，则在连接成功之后会发送 routing_id_msg。这是 engine 的处理逻辑，和 socket 无关(具体实现参考 _next_msg/_process_msg)
    // FIXME: 本模块中关于 routing_id message 消息的发送与接收流程待确定？？？
    _next_msg = static_cast<int (stream_engine_base_t::*)(msg_t *)>(&zmtp_engine_t::routing_id_msg);
    _process_msg = static_cast<int (stream_engine_base_t::*)(msg_t *)>(&zmtp_engine_t::process_routing_id_msg);

    int rc = _pong_msg.init();
    errno_assert(rc == 0);

    rc = _routing_id_msg.init();
    errno_assert(rc == 0);

    if (_options.heartbeat_interval > 0)
    {
        _heartbeat_timeout = _options.heartbeat_timeout;
        if (_heartbeat_timeout == -1)
            _heartbeat_timeout = _options.heartbeat_interval;
    }
}

zmq::zmtp_engine_t::~zmtp_engine_t()
{
    const int rc = _routing_id_msg.close();
    errno_assert(rc == 0);
}

void zmq::zmtp_engine_t::plug_internal()
{
    // start optional timer, to prevent handshake hanging on no input
    set_handshake_timer();

    //  Send the 'length' and 'flags' fields of the routing id message.
    //  The 'length' field is encoded in the long format.
    // 构造 greeting 消息（当向内核注册 conn_fd 之后，如果 conn_fd 可写，会在 out_event() 中发送 greeting 消息）
    _outpos = _greeting_send;
    _outpos[_outsize++] = UCHAR_MAX;        // 0xff
    put_uint64(&_outpos[_outsize], _options.routing_id_size + 1);
    _outsize += 8;
    _outpos[_outsize++] = 0x7f;

    set_pollin();       // 设置 conn_fd 可读事件回调函数。当 conn_fd 可读时，从 socket 读取 greeting 消息
    set_pollout();      // 设置 conn_fd 可写事件回调函数。当 conn_fd 可写时，从 socket 写入 greeting 消息
    //  Flush all the data that may have been already received downstream.
    /**
     * @brief 对端可能已经写入了数据，所以在这里直接调用 in_event() 处理数据，不用等待 epoll 返回（比如处理对端发送过来的 greeting 消息）
     *
     * 比如 tcp 通信中，如果 server 端启动顺序先于 cliend 端；则当 client 端的 conn_fd 还未被加入到内核监听队列(未调用到 add_fd 和 set_pollout)，server 端的 conn_fd 的 EPOLLOUT 事件就已经被触发了(这是因为当 client 端 connect 成功之后，server 端就已经可写了)，server 端就会发送 greeting 消息过来。所以对于此时的 client 来说，不需要等到 epoll 返回再去读取数据，而是可以直接尝试从内核读取数据。
     */
    in_event();
}

//  Position of the revision and minor fields in the greeting.
const size_t revision_pos = 10;
const size_t minor_pos = 11;
#include <iostream>
bool zmq::zmtp_engine_t::handshake()
{
    zmq_assert(_greeting_bytes_read < _greeting_size);
    //  Receive the greeting.
    const int rc = receive_greeting();
    if (rc == -1)
        return false;
    const bool unversioned = rc != 0;

    // select_handshake_fun 中会实例化 _encoder，_decoder 以及 _mechanism，并设置函数指针 _process_msg 和 _next_msg
    if (!(this->*select_handshake_fun(unversioned, _greeting_recv[revision_pos], _greeting_recv[minor_pos]))())
        return false;

    // Start polling for output if necessary.
    if (_outsize == 0)
        set_pollout();

    return true;
}

int zmq::zmtp_engine_t::receive_greeting()
{
    bool unversioned = false;
    while (_greeting_bytes_read < _greeting_size)
    {
        /**
         * server 端：
         * 1. client 发送的第一条 greeting 消息长度为 10 字节，但是 server 在第一次循环中读取了 10 字节，在第二次循环中读取 2 字节失败，return -1
         * 2. client 发送的第二条 greeting 消息长度为 64 字节，第一次循环读取 2 字节成功，第二次循环读取 64 - 2 - 10 = 52 字节成功，return unversioned ? 1 : 0;
         *
         * client 端逻辑类似。
         */
        const int n = read(_greeting_recv + _greeting_bytes_read, _greeting_size - _greeting_bytes_read);
        if (n == -1)
        {
            if (errno != EAGAIN)
                error(connection_error);
            return -1;
        }

        _greeting_bytes_read += n;

        //  We have received at least one byte from the peer.
        //  If the first byte is not 0xff, we know that the
        //  peer is using unversioned protocol.
        if (_greeting_recv[0] != 0xff)
        {
            unversioned = true;
            break;
        }

        if (_greeting_bytes_read < signature_size)
            continue;

        //  Inspect the right-most bit of the 10th byte (which coincides
        //  with the 'flags' field if a regular message was sent).
        //  Zero indicates this is a header of a routing id message
        //  (i.e. the peer is using the unversioned protocol).
        if (!(_greeting_recv[9] & 0x01))
        {
            unversioned = true;
            break;
        }

        //  The peer is using versioned protocol.
        receive_greeting_versioned();
    }
    return unversioned ? 1 : 0;
}

/**
 * 这里需要特别注意的是：ZMTP 的 greeting 消息并不是只有一条，而是类似于三次握手
 *
 * 1. client--->server client 发送给 server 第一条 greeting 消息：消息长度_outsize为 10 字节。消息已经在 plug_internal 中提前构造好了
 * 2. server 收到 greeting 的消息后，会在自己构造出来的 greeting 消息尾部(即第十个字节的位置)增加一个字节(标明主版本号)，发送第一条 greeting 消息给 client，消息长度_outsize为 11 字节
 * 3. client 收到 server 发送过来的长度为 11 字节的 greeting 消息，读取 _greeting_recv[revision_pos]，判断 server 的主版本号。然后构造第二条 greeting 消息（将次版本号写入到消息体的第十一个字节），消息体长度_outsize为 64 字节
 * 4. server 端收到 client 端发送过来的第二条 greeting 消息，但是由于第二条 greeting 消息长度为 64 字节，而 server 端的 _greeting_size 为 12，所以 server 无法全部接收 client 发送的第二条 greeting 消息，只能读取到 12 字节。构造第二条发送给 client 端的 greeting 消息(将次版本号写入到消息体的第一个字节)，消息体长度为 64 字节，同时更新自己的 _greeting_size 为 64
 *    4.1 server 端由于没有全部读取从 client 发送过来的第二条 greeting 消息，所以会第二次触发 EPOLLIN 事件。这次读取的消息长度为 64 字节。至此，server 端已经准备好。
 * 5. client 收到 server 端发送过来的第二条 greeting 消息，消息体长度为 64 字节。至此 client 端已准备好。
 *
 * 综上，client 端发送给 server 两条 greeting 消息；server 发送给 client 两条 greeting 消息
 *
 * 注意：每次发送完 greeting 消息之后，都会将 EPOLLOUT 事件从内核监听队列中删除，所以需要重新发送 greeting 消息之前，需要先监听 EPOLLOUT 事件
 */
void zmq::zmtp_engine_t::receive_greeting_versioned()
{
    //  Send the major version number.
    if (_outpos + _outsize == _greeting_send + signature_size)
    {
        if (_outsize == 0)      // 只有当本端 greeting 数据全部发送完毕之后，_outsize 会被置为0（out_event 中发送数据）
            set_pollout();      // 重新监听 conn_fd 的可写事件，发送下一条 greeting 消息
        _outpos[_outsize++] = 3; //  Major version number（如果本端 greeting 消息未发送，则发送给对端的第一条 greeting 消息将是 11 字节）
    }

    if (_greeting_bytes_read > signature_size)
    {
        if (_outpos + _outsize == _greeting_send + signature_size + 1)
        {
            if (_outsize == 0)
                set_pollout();

            //  Use ZMTP/2.0 to talk to older peers.
            if (_greeting_recv[revision_pos] == ZMTP_1_0 || _greeting_recv[revision_pos] == ZMTP_2_0)
                _outpos[_outsize++] = _options.type;
            else
            {
                _outpos[_outsize++] = 1; //  Minor version number
                memset(_outpos + _outsize, 0, 20);

                // clang-format off
                zmq_assert(_options.mechanism == ZMQ_NULL
                        || _options.mechanism == ZMQ_PLAIN
                        || _options.mechanism == ZMQ_CURVE
                        || _options.mechanism == ZMQ_GSSAPI);
                // clang-format on

                if (_options.mechanism == ZMQ_NULL)
                    memcpy(_outpos + _outsize, "NULL", 4);
                else if (_options.mechanism == ZMQ_PLAIN)
                    memcpy(_outpos + _outsize, "PLAIN", 5);
                else if (_options.mechanism == ZMQ_GSSAPI)
                    memcpy(_outpos + _outsize, "GSSAPI", 6);
                else if (_options.mechanism == ZMQ_CURVE)
                    memcpy(_outpos + _outsize, "CURVE", 5);
                _outsize += 20;
                memset(_outpos + _outsize, 0, 32);
                _outsize += 32;
                _greeting_size = v3_greeting_size;
            }
        }
    }
}

zmq::zmtp_engine_t::handshake_fun_t zmq::zmtp_engine_t::select_handshake_fun(bool unversioned_, unsigned char revision_, unsigned char minor_)
{
    //  Is the peer using ZMTP/1.0 with no revision number?
    if (unversioned_)
    {
        return &zmtp_engine_t::handshake_v1_0_unversioned;
    }
    switch (revision_)
    {
    case ZMTP_1_0:
        return &zmtp_engine_t::handshake_v1_0;
    case ZMTP_2_0:
        return &zmtp_engine_t::handshake_v2_0;
    case ZMTP_3_x:
        switch (minor_)
        {
        case 0:
            return &zmtp_engine_t::handshake_v3_0;
        default:
            return &zmtp_engine_t::handshake_v3_1;
        }
    default:
        return &zmtp_engine_t::handshake_v3_1;
    }
}

bool zmq::zmtp_engine_t::handshake_v1_0_unversioned()
{
    //  We send and receive rest of routing id message
    if (session()->zap_enabled())
    {
        // reject ZMTP 1.0 connections if ZAP is enabled
        error(protocol_error);
        return false;
    }

    _encoder = new (std::nothrow) v1_encoder_t(_options.out_batch_size);
    alloc_assert(_encoder);

    _decoder = new (std::nothrow) v1_decoder_t(_options.in_batch_size, _options.maxmsgsize);
    alloc_assert(_decoder);

    //  We have already sent the message header.
    //  Since there is no way to tell the encoder to
    //  skip the message header, we simply throw that
    //  header data away.
    const size_t header_size = _options.routing_id_size + 1 >= UCHAR_MAX ? 10 : 2;
    unsigned char tmp[10], *bufferp = tmp;

    //  Prepare the routing id message and load it into encoder.
    //  Then consume bytes we have already sent to the peer.
    int rc = _routing_id_msg.close();
    zmq_assert(rc == 0);
    rc = _routing_id_msg.init_size(_options.routing_id_size);
    zmq_assert(rc == 0);
    memcpy(_routing_id_msg.data(), _options.routing_id, _options.routing_id_size);
    _encoder->load_msg(&_routing_id_msg);
    const size_t buffer_size = _encoder->encode(&bufferp, header_size);
    zmq_assert(buffer_size == header_size);

    //  Make sure the decoder sees the data we have already received.
    _inpos = _greeting_recv;
    _insize = _greeting_bytes_read;

    //  To allow for interoperability with peers that do not forward
    //  their subscriptions, we inject a phantom subscription message
    //  message into the incoming message stream.
    if (_options.type == ZMQ_PUB || _options.type == ZMQ_XPUB)
        _subscription_required = true;

    //  We are sending our routing id now and the next message
    //  will come from the socket.
    _next_msg = &zmtp_engine_t::pull_msg_from_session;

    //  We are expecting routing id message.
    _process_msg = static_cast<int (stream_engine_base_t::*)(msg_t *)>(&zmtp_engine_t::process_routing_id_msg);

    return true;
}

bool zmq::zmtp_engine_t::handshake_v1_0()
{
    if (session()->zap_enabled())
    {
        // reject ZMTP 1.0 connections if ZAP is enabled
        error(protocol_error);
        return false;
    }

    _encoder = new (std::nothrow) v1_encoder_t(_options.out_batch_size);
    alloc_assert(_encoder);

    _decoder = new (std::nothrow) v1_decoder_t(_options.in_batch_size, _options.maxmsgsize);
    alloc_assert(_decoder);

    return true;
}

bool zmq::zmtp_engine_t::handshake_v2_0()
{
    if (session()->zap_enabled())
    {
        // reject ZMTP 2.0 connections if ZAP is enabled
        error(protocol_error);
        return false;
    }

    _encoder = new (std::nothrow) v2_encoder_t(_options.out_batch_size);
    alloc_assert(_encoder);

    _decoder = new (std::nothrow) v2_decoder_t(_options.in_batch_size, _options.maxmsgsize, _options.zero_copy);
    alloc_assert(_decoder);

    return true;
}

bool zmq::zmtp_engine_t::handshake_v3_x(const bool downgrade_sub_)
{
    if (_options.mechanism == ZMQ_NULL && memcmp(_greeting_recv + 12, "NULL\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 20) == 0)
    {
        _mechanism = new (std::nothrow) null_mechanism_t(session(), _peer_address, _options);
        alloc_assert(_mechanism);
    }
    else if (_options.mechanism == ZMQ_PLAIN && memcmp(_greeting_recv + 12, "PLAIN\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 20) == 0)
    {
        if (_options.as_server)
            _mechanism = new (std::nothrow) plain_server_t(session(), _peer_address, _options);
        else
            _mechanism = new (std::nothrow) plain_client_t(session(), _options);
        alloc_assert(_mechanism);
    }
#ifdef ZMQ_HAVE_CURVE
    else if (_options.mechanism == ZMQ_CURVE && memcmp(_greeting_recv + 12, "CURVE\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 20) == 0)
    {
        if (_options.as_server)
            _mechanism = new (std::nothrow) curve_server_t(session(), _peer_address, _options, downgrade_sub_);
        else
            _mechanism = new (std::nothrow) curve_client_t(session(), _options, downgrade_sub_);
        alloc_assert(_mechanism);
    }
#endif
#ifdef HAVE_LIBGSSAPI_KRB5
    else if (_options.mechanism == ZMQ_GSSAPI && memcmp(_greeting_recv + 12, "GSSAPI\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 20) == 0)
    {
        if (_options.as_server)
            _mechanism = new (std::nothrow) gssapi_server_t(session(), _peer_address, _options);
        else
            _mechanism = new (std::nothrow) gssapi_client_t(session(), _options);
        alloc_assert(_mechanism);
    }
#endif
    else
    {
        socket()->event_handshake_failed_protocol(session()->get_endpoint(), ZMQ_PROTOCOL_ERROR_ZMTP_MECHANISM_MISMATCH);
        error(protocol_error);
        return false;
    }
    _next_msg = &zmtp_engine_t::next_handshake_command;
    _process_msg = &zmtp_engine_t::process_handshake_command;

    return true;
}

bool zmq::zmtp_engine_t::handshake_v3_0()
{
    _encoder = new (std::nothrow) v2_encoder_t(_options.out_batch_size);
    alloc_assert(_encoder);

    _decoder = new (std::nothrow) v2_decoder_t(_options.in_batch_size, _options.maxmsgsize, _options.zero_copy);
    alloc_assert(_decoder);

    return zmq::zmtp_engine_t::handshake_v3_x(true);
}

bool zmq::zmtp_engine_t::handshake_v3_1()
{
    _encoder = new (std::nothrow) v3_1_encoder_t(_options.out_batch_size);
    alloc_assert(_encoder);

    _decoder = new (std::nothrow) v2_decoder_t(_options.in_batch_size, _options.maxmsgsize, _options.zero_copy);
    alloc_assert(_decoder);

    return zmq::zmtp_engine_t::handshake_v3_x(false);
}

// 连接刚建立的时候，会由 engine 构造一条 routing_id 消息发送给对端socket，由对端socket使用该 routing_id 标记本端 socket，达到路由目的
// 发送 routing_id_msg 是在 handshake() 之后的
int zmq::zmtp_engine_t::routing_id_msg(msg_t *msg_)
{
    const int rc = msg_->init_size(_options.routing_id_size);
    errno_assert(rc == 0);
    // socket 可以使用 zmq_setsockopt 设置 routing_id，如果没有设置，则会发送一条空消息，对端收到之后会生成一个 UUID 来标记本端socket
    if (_options.routing_id_size > 0)
        memcpy(msg_->data(), _options.routing_id, _options.routing_id_size);
    _next_msg = &zmtp_engine_t::pull_msg_from_session;      // 改变函数指针，进行正常的数据交互
    return 0;
}

// 如果是 zmtp 协议，则在连接刚建立时，对端socket会发送过来一条 routing_id 的消息，接收到该消息，并为对方建立 UUID（这就是 “信封”）
int zmq::zmtp_engine_t::process_routing_id_msg(msg_t *msg_)
{
    if (_options.recv_routing_id)
    {
        msg_->set_flags(msg_t::routing_id);
        const int rc = session()->push_msg(msg_);
        errno_assert(rc == 0);
    }
    else
    {
        int rc = msg_->close();
        errno_assert(rc == 0);
        rc = msg_->init();
        errno_assert(rc == 0);
    }

    if (_subscription_required)
    {
        msg_t subscription;

        //  Inject the subscription message, so that also
        //  ZMQ 2.x peers receive published messages.
        int rc = subscription.init_size(1);
        errno_assert(rc == 0);
        *static_cast<unsigned char *>(subscription.data()) = 1;
        rc = session()->push_msg(&subscription);
        errno_assert(rc == 0);
    }

    _process_msg = &zmtp_engine_t::push_msg_to_session;     // 改变函数指针，进行正常的数据交互

    return 0;
}

int zmq::zmtp_engine_t::produce_ping_message(msg_t *msg_)
{
    // 16-bit TTL + \4PING == 7
    const size_t ping_ttl_len = msg_t::ping_cmd_name_size + 2;
    zmq_assert(_mechanism != NULL);

    int rc = msg_->init_size(ping_ttl_len);
    errno_assert(rc == 0);
    msg_->set_flags(msg_t::command);
    // Copy in the command message
    memcpy(msg_->data(), "\4PING", msg_t::ping_cmd_name_size);

    uint16_t ttl_val = htons(_options.heartbeat_ttl);
    memcpy(static_cast<uint8_t *>(msg_->data()) + msg_t::ping_cmd_name_size, &ttl_val, sizeof(ttl_val));

    rc = _mechanism->encode(msg_);
    _next_msg = &zmtp_engine_t::pull_and_encode;
    if (!_has_timeout_timer && _heartbeat_timeout > 0)
    {
        add_timer(_heartbeat_timeout, heartbeat_timeout_timer_id);
        _has_timeout_timer = true;
    }
    return rc;
}

int zmq::zmtp_engine_t::produce_pong_message(msg_t *msg_)
{
    zmq_assert(_mechanism != NULL);

    int rc = msg_->move(_pong_msg);
    errno_assert(rc == 0);

    rc = _mechanism->encode(msg_);
    _next_msg = &zmtp_engine_t::pull_and_encode;
    return rc;
}

int zmq::zmtp_engine_t::process_heartbeat_message(msg_t *msg_)
{
    if (msg_->is_ping())
    {
        // 16-bit TTL + \4PING == 7
        const size_t ping_ttl_len = msg_t::ping_cmd_name_size + 2;
        const size_t ping_max_ctx_len = 16;
        uint16_t remote_heartbeat_ttl;

        // Get the remote heartbeat TTL to setup the timer
        memcpy(&remote_heartbeat_ttl, static_cast<uint8_t *>(msg_->data()) + msg_t::ping_cmd_name_size, ping_ttl_len - msg_t::ping_cmd_name_size);
        remote_heartbeat_ttl = ntohs(remote_heartbeat_ttl);
        // The remote heartbeat is in 10ths of a second
        // so we multiply it by 100 to get the timer interval in ms.
        remote_heartbeat_ttl *= 100;

        if (!_has_ttl_timer && remote_heartbeat_ttl > 0)
        {
            add_timer(remote_heartbeat_ttl, heartbeat_ttl_timer_id);
            _has_ttl_timer = true;
        }

        //  As per ZMTP 3.1 the PING command might contain an up to 16 bytes
        //  context which needs to be PONGed back, so build the pong message
        //  here and store it. Truncate it if it's too long.
        //  Given the engine goes straight to out_event, sequential PINGs will
        //  not be a problem.
        const size_t context_len = std::min(msg_->size() - ping_ttl_len, ping_max_ctx_len);
        const int rc = _pong_msg.init_size(msg_t::ping_cmd_name_size + context_len);
        errno_assert(rc == 0);
        _pong_msg.set_flags(msg_t::command);
        memcpy(_pong_msg.data(), "\4PONG", msg_t::ping_cmd_name_size);
        if (context_len > 0)
            memcpy(static_cast<uint8_t *>(_pong_msg.data()) + msg_t::ping_cmd_name_size, static_cast<uint8_t *>(msg_->data()) + ping_ttl_len,
                context_len);

        _next_msg = static_cast<int (stream_engine_base_t::*)(msg_t *)>(&zmtp_engine_t::produce_pong_message);
        out_event();
    }

    return 0;
}

int zmq::zmtp_engine_t::process_command_message(msg_t *msg_)
{
    const uint8_t cmd_name_size = *(static_cast<const uint8_t *>(msg_->data()));
    const size_t ping_name_size = msg_t::ping_cmd_name_size - 1;
    const size_t sub_name_size = msg_t::sub_cmd_name_size - 1;
    const size_t cancel_name_size = msg_t::cancel_cmd_name_size - 1;
    //  Malformed command
    if (unlikely(msg_->size() < cmd_name_size + sizeof(cmd_name_size)))
        return -1;

    const uint8_t *const cmd_name = static_cast<const uint8_t *>(msg_->data()) + 1;
    if (cmd_name_size == ping_name_size && memcmp(cmd_name, "PING", cmd_name_size) == 0)
        msg_->set_flags(zmq::msg_t::ping);
    if (cmd_name_size == ping_name_size && memcmp(cmd_name, "PONG", cmd_name_size) == 0)
        msg_->set_flags(zmq::msg_t::pong);
    if (cmd_name_size == sub_name_size && memcmp(cmd_name, "SUBSCRIBE", cmd_name_size) == 0)
        msg_->set_flags(zmq::msg_t::subscribe);
    if (cmd_name_size == cancel_name_size && memcmp(cmd_name, "CANCEL", cmd_name_size) == 0)
        msg_->set_flags(zmq::msg_t::cancel);

    if (msg_->is_ping() || msg_->is_pong())
        return process_heartbeat_message(msg_);

    return 0;
}
