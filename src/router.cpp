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
#include "router.hpp"
#include "pipe.hpp"
#include "wire.hpp"
#include "random.hpp"
#include "likely.hpp"
#include "err.hpp"

zmq::router_t::router_t(class ctx_t *parent_, uint32_t tid_, int sid_)
    : routing_socket_base_t(parent_, tid_, sid_)
    , _prefetched(false)
    , _routing_id_sent(false)
    , _current_in(NULL)
    , _terminate_current_in(false)
    , _more_in(false)
    , _current_out(NULL)
    , _more_out(false)
    , _next_integral_routing_id(generate_random())
    , _mandatory(false)
    ,
    //  raw_socket functionality in ROUTER is deprecated
    _raw_socket(false)
    , _probe_router(false)
    , _handover(false)
{
    options.type = ZMQ_ROUTER;
    options.recv_routing_id = true;
    options.raw_socket = false;
    options.can_send_hello_msg = true;
    options.can_recv_disconnect_msg = true;

    _prefetched_id.init();
    _prefetched_msg.init();
}

zmq::router_t::~router_t()
{
    zmq_assert(_anonymous_pipes.empty());
    _prefetched_id.close();
    _prefetched_msg.close();
}

/**
 * ROUTER在收到消息时会在顶部添加一个信封，标记消息来源。发送时会通过该信封决定哪个节点可以获取到该条消息。
 */

/**
 * 如果 router 做客户端，会在 socket_base_t::connect --> xattach_pipe，但是在这里 routing_id_ok 会为 false(因为还没读取到对端发送过来的空消息，无法生成 UUID)，所以不会 _fq.attach(pipe_)。当连接完全建立之后，对端会发送空消息，然后 router 在 router_t::xread_activated 中读取空消息，为对端生成 UUID 标识，并执行 _fq.attach(pipe_)
 *
 * 如果 router 做服务端，会在连接建立之后，session_base_t::engine_ready 创建 pipe，并给 socket 发送 bind 命令。socket 在 zmq_recv 的时候会处理该命令，然后读取对端发送过来的空消息，并为其生成 UUID
 *
 * 注意：rep 继承于 router
 */
void zmq::router_t::xattach_pipe(pipe_t *pipe_, bool subscribe_to_all_, bool locally_initiated_)
{
    LIBZMQ_UNUSED(subscribe_to_all_);

    zmq_assert(pipe_);

    if (_probe_router)          // 如果设置了 ZMQ_PROBE_ROUTER，则会给对端发送(带有套接字标识)空消息
    {
        msg_t probe_msg;
        int rc = probe_msg.init();
        errno_assert(rc == 0);

        // 注意：这里发送消息，只是将消息写给了和 socket 通信的管道。后面会由 session 从 pipe 读取消息，并交给 engine 发送给内核
        rc = pipe_->write(&probe_msg);
        // zmq_assert (rc) is not applicable here, since it is not a bug.
        LIBZMQ_UNUSED(rc);

        pipe_->flush();

        rc = probe_msg.close();
        errno_assert(rc == 0);
    }

    const bool routing_id_ok = identify_peer(pipe_, locally_initiated_);    // 识别对端 routing_id（会读取消息）
    if (routing_id_ok)  // 如果对端确实发送了空消息或者含有 routing_id 的消息（连接刚建立之后，zmtp_engine 会发送 routing_id 的消息给 router 生成 UUID，用于标识对方 socket）
        _fq.attach(pipe_);
    else
        _anonymous_pipes.insert(pipe_);
}

int zmq::router_t::xsetsockopt(int option_, const void *optval_, size_t optvallen_)
{
    const bool is_int = (optvallen_ == sizeof(int));
    int value = 0;
    if (is_int)
        memcpy(&value, optval_, sizeof(int));

    switch (option_)
    {
    case ZMQ_ROUTER_RAW:
        if (is_int && value >= 0)
        {
            _raw_socket = (value != 0);
            if (_raw_socket)
            {
                options.recv_routing_id = false;
                options.raw_socket = true;
            }
            return 0;
        }
        break;

    case ZMQ_ROUTER_MANDATORY:
        if (is_int && value >= 0)
        {
            _mandatory = (value != 0);
            return 0;
        }
        break;

    case ZMQ_PROBE_ROUTER:
        if (is_int && value >= 0)
        {
            _probe_router = (value != 0);
            return 0;
        }
        break;

    case ZMQ_ROUTER_HANDOVER:
        if (is_int && value >= 0)
        {
            _handover = (value != 0);
            return 0;
        }
        break;

#ifdef ZMQ_BUILD_DRAFT_API
    case ZMQ_ROUTER_NOTIFY:
        if (is_int && value >= 0 && value <= (ZMQ_NOTIFY_CONNECT | ZMQ_NOTIFY_DISCONNECT))
        {
            options.router_notify = value;
            return 0;
        }
        break;
#endif

    default:
        return routing_socket_base_t::xsetsockopt(option_, optval_, optvallen_);
    }
    errno = EINVAL;
    return -1;
}

void zmq::router_t::xpipe_terminated(pipe_t *pipe_)
{
    if (0 == _anonymous_pipes.erase(pipe_))
    {
        erase_out_pipe(pipe_);
        _fq.pipe_terminated(pipe_);
        pipe_->rollback();
        if (pipe_ == _current_out)
            _current_out = NULL;
    }
}

// 如果 router 是客户端，会在这里读取对端发送过来的空消息
// 如果 sessoin 向 pipe 中写入了数据，则会发送命令给 socket，socket 在 zmq_recv 之前会先处理该命令
void zmq::router_t::xread_activated(pipe_t *pipe_)
{
    const std::set<pipe_t *>::iterator it = _anonymous_pipes.find(pipe_);
    if (it == _anonymous_pipes.end())
        _fq.activated(pipe_);       // 将该 pipe 标记为活动的
    else
    {
        const bool routing_id_ok = identify_peer(pipe_, false);
        if (routing_id_ok)
        {
            _anonymous_pipes.erase(it);
            _fq.attach(pipe_);
        }
    }
}

int zmq::router_t::xsend(msg_t *msg_)
{
    //  If this is the first part of the message it's the ID of the
    //  peer to send the message to.
    if (!_more_out)
    {
        zmq_assert(!_current_out);

        //  If we have malformed message (prefix with no subsequent message)
        //  then just silently ignore it.
        //  TODO: The connections should be killed instead.
        if (msg_->flags() & msg_t::more)
        {
            _more_out = true;

            //  Find the pipe associated with the routing id stored in the prefix.
            //  If there's no such pipe just silently ignore the message, unless
            //  router_mandatory is set.
            // 该消息是含有套接字标识(routing_id)的消息，主要是用来寻找路由方向(发送给哪个套接字)，所以找到之后就无需发送了，直接关闭
            // 也就是说，该条消息实际上并没有被发送出去，对端也无法接收到该条消息（如果对端还没有连接上来，router 找不到路由方向，会丢弃后续消息）
            out_pipe_t *out_pipe = lookup_out_pipe(blob_t(static_cast<unsigned char *>(msg_->data()), msg_->size(), zmq::reference_tag_t()));

            if (out_pipe)
            {
                _current_out = out_pipe->pipe;

                // Check whether pipe is closed or not
                if (!_current_out->check_write())
                {
                    // Check whether pipe is full or not
                    const bool pipe_full = !_current_out->check_hwm();
                    out_pipe->active = false;
                    _current_out = NULL;

                    if (_mandatory)
                    {
                        _more_out = false;
                        if (pipe_full)
                            errno = EAGAIN;
                        else
                            errno = EHOSTUNREACH;
                        return -1;
                    }
                }
            }
            else if (_mandatory)
            {
                _more_out = false;
                errno = EHOSTUNREACH;
                return -1;
            }
        }

        int rc = msg_->close();
        errno_assert(rc == 0);
        rc = msg_->init();
        errno_assert(rc == 0);
        return 0;
    }

    //  Ignore the MORE flag for raw-sock or assert?
    if (options.raw_socket)
        msg_->reset_flags(msg_t::more);

    //  Check whether this is the last part of the message.
    _more_out = (msg_->flags() & msg_t::more) != 0;

    //  Push the message into the pipe. If there's no out pipe, just drop it.
    if (_current_out)   // 已经找到了路由
    {
        // Close the remote connection if user has asked to do so
        // by sending zero length message.
        // Pending messages in the pipe will be dropped (on receiving term- ack)
        if (_raw_socket && msg_->size() == 0)
        {
            _current_out->terminate(false);
            int rc = msg_->close();
            errno_assert(rc == 0);
            rc = msg_->init();
            errno_assert(rc == 0);
            _current_out = NULL;
            return 0;
        }

        const bool ok = _current_out->write(msg_);      // 发送数据
        if (unlikely(!ok))
        {
            // Message failed to send - we must close it ourselves.
            const int rc = msg_->close();
            errno_assert(rc == 0);
            // HWM was checked before, so the pipe must be gone. Roll back
            // messages that were piped, for example REP labels.
            _current_out->rollback();
            _current_out = NULL;
        }
        else
        {
            if (!_more_out)
            {
                _current_out->flush();
                _current_out = NULL;
            }
        }
    }
    else
    {
        // 没有找到路由，直接丢弃消息
        const int rc = msg_->close();
        errno_assert(rc == 0);
    }

    //  Detach the message from the data buffer.
    const int rc = msg_->init();
    errno_assert(rc == 0);

    return 0;
}

int zmq::router_t::xrecv(msg_t *msg_)
{
    if (_prefetched)
    {
        // 在这里会将保存的空消息发送给应用程序
        if (!_routing_id_sent)
        {
            const int rc = msg_->move(_prefetched_id);
            errno_assert(rc == 0);
            _routing_id_sent = true;
        }
        else
        {
            const int rc = msg_->move(_prefetched_msg);
            errno_assert(rc == 0);
            _prefetched = false;
        }
        _more_in = (msg_->flags() & msg_t::more) != 0;

        if (!_more_in)
        {
            if (_terminate_current_in)
            {
                _current_in->terminate(true);
                _terminate_current_in = false;
            }
            _current_in = NULL;
        }
        return 0;
    }

    pipe_t *pipe = NULL;
    int rc = _fq.recvpipe(msg_, &pipe);

    //  It's possible that we receive peer's routing id. That happens
    //  after reconnection. The current implementation assumes that
    //  the peer always uses the same routing id.
    while (rc == 0 && msg_->is_routing_id())
        rc = _fq.recvpipe(msg_, &pipe);

    // 可能读取到空消息，rc == 0（req 或者 router 会发送一个空消息给 router）

    if (rc != 0)    // 没有完成 attach_pipe 的操作，所以读取不到消息
        return -1;

    zmq_assert(pipe != NULL);

    //  If we are in the middle of reading a message, just return the next part.
    if (_more_in)
    {
        _more_in = (msg_->flags() & msg_t::more) != 0;

        if (!_more_in)
        {
            if (_terminate_current_in)
            {
                _current_in->terminate(true);
                _terminate_current_in = false;
            }
            _current_in = NULL;
        }
    }
    else
    {
        // ROUTER 接收到消息时，会在顶部添加一个信封，标记消息的来源
        // 也就是说，如果 req 发送了三帧消息，但是用户需要从 router 读取四次才可以（第一次是 router 添加的信封）

        //  We are at the beginning of a message.
        //  Keep the message part we have in the prefetch buffer
        //  and return the ID of the peer instead.
        rc = _prefetched_msg.move(*msg_);       // 保存接收到的消息，然后将 UUID 返回给用户，标记消息来源（第二次调用 recv 才返回这次接收到的消息）
        errno_assert(rc == 0);
        _prefetched = true;
        _current_in = pipe;     // 接收到对端发送过来的空消息，会用来寻找 pipe，然后用该 pipe 接收剩下的真实的用户消息

        const blob_t &routing_id = pipe->get_routing_id();      // pipe 的 UUID 已经在 identify_peer 调用中保存下来了
        rc = msg_->init_size(routing_id.size());
        errno_assert(rc == 0);
        memcpy(msg_->data(), routing_id.data(), routing_id.size());     // 将 routing_id 作为消息返回给用户
        msg_->set_flags(msg_t::more);
        if (_prefetched_msg.metadata())
            msg_->set_metadata(_prefetched_msg.metadata());
        _routing_id_sent = true;
    }

    return 0;
}

int zmq::router_t::rollback()
{
    if (_current_out)
    {
        _current_out->rollback();
        _current_out = NULL;
        _more_out = false;
    }
    return 0;
}

bool zmq::router_t::xhas_in()
{
    //  If we are in the middle of reading the messages, there are
    //  definitely more parts available.
    if (_more_in)
        return true;

    //  We may already have a message pre-fetched.
    if (_prefetched)
        return true;

    //  Try to read the next message.
    //  The message, if read, is kept in the pre-fetch buffer.
    pipe_t *pipe = NULL;
    int rc = _fq.recvpipe(&_prefetched_msg, &pipe);

    //  It's possible that we receive peer's routing id. That happens
    //  after reconnection. The current implementation assumes that
    //  the peer always uses the same routing id.
    //  TODO: handle the situation when the peer changes its routing id.
    while (rc == 0 && _prefetched_msg.is_routing_id())
        rc = _fq.recvpipe(&_prefetched_msg, &pipe);

    if (rc != 0)
        return false;

    zmq_assert(pipe != NULL);

    const blob_t &routing_id = pipe->get_routing_id();
    rc = _prefetched_id.init_size(routing_id.size());
    errno_assert(rc == 0);
    memcpy(_prefetched_id.data(), routing_id.data(), routing_id.size());
    _prefetched_id.set_flags(msg_t::more);

    _prefetched = true;
    _routing_id_sent = false;
    _current_in = pipe;

    return true;
}

static bool check_pipe_hwm(const zmq::pipe_t &pipe_)
{
    return pipe_.check_hwm();
}

bool zmq::router_t::xhas_out()
{
    //  In theory, ROUTER socket is always ready for writing (except when
    //  MANDATORY is set). Whether actual attempt to write succeeds depends
    //  on whitch pipe the message is going to be routed to.

    if (!_mandatory)
        return true;

    return any_of_out_pipes(check_pipe_hwm);
}

int zmq::router_t::get_peer_state(const void *routing_id_, size_t routing_id_size_) const
{
    int res = 0;

    // TODO remove the const_cast, see comment in lookup_out_pipe
    const blob_t routing_id_blob(static_cast<unsigned char *>(const_cast<void *>(routing_id_)), routing_id_size_, reference_tag_t());
    const out_pipe_t *out_pipe = lookup_out_pipe(routing_id_blob);
    if (!out_pipe)
    {
        errno = EHOSTUNREACH;
        return -1;
    }

    if (out_pipe->pipe->check_hwm())
        res |= ZMQ_POLLOUT;

    /** \todo does it make any sense to check the inpipe as well? */

    return res;
}

bool zmq::router_t::identify_peer(pipe_t *pipe_, bool locally_initiated_)
{
    msg_t msg;
    blob_t routing_id;

    if (locally_initiated_ && connect_routing_id_is_set())
    {
        const std::string connect_routing_id = extract_connect_routing_id();
        routing_id.set(reinterpret_cast<const unsigned char *>(connect_routing_id.c_str()), connect_routing_id.length());
        //  Not allowed to duplicate an existing rid
        zmq_assert(!has_out_pipe(routing_id));
    }
    else if (options.raw_socket)
    { //  Always assign an integral routing id for raw-socket
        unsigned char buf[5];
        buf[0] = 0;
        put_uint32(buf + 1, _next_integral_routing_id++);
        routing_id.set(buf, sizeof buf);
    }
    else if (!options.raw_socket)
    {
        //  Pick up handshake cases and also case where next integral routing id is set
        msg.init();
        const bool ok = pipe_->read(&msg);
        if (!ok)
            return false;

        if (msg.size() == 0)
        {
            // 如果对端发送过来的空消息中没有套接字标识(routing_id), 则 router 会自动生成一个 UUID 来标识消息的来源
            //  Fall back on the auto-generation
            unsigned char buf[5];
            buf[0] = 0;
            put_uint32(buf + 1, _next_integral_routing_id++);       // 自动生成 UUID 标识消息来源
            routing_id.set(buf, sizeof buf);        // 设置 routing_id, 长度为 5 字节
            msg.close();
        }
        else
        {
            // 对端发送空消息中含有 routing_id，用该 routing_id 来标识消息来源
            routing_id.set(static_cast<unsigned char *>(msg.data()), msg.size());
            msg.close();

            //  Try to remove an existing routing id entry to allow the new
            //  connection to take the routing id.
            const out_pipe_t *const existing_outpipe = lookup_out_pipe(routing_id);

            if (existing_outpipe)       // 如果这个 routing_id 已经存在，则会覆盖（如果允许覆盖才会覆盖）
            {
                if (!_handover)
                    //  Ignore peers with duplicate ID
                    return false;

                //  We will allow the new connection to take over this
                //  routing id. Temporarily assign a new routing id to the
                //  existing pipe so we can terminate it asynchronously.
                unsigned char buf[5];
                buf[0] = 0;
                put_uint32(buf + 1, _next_integral_routing_id++);
                blob_t new_routing_id(buf, sizeof buf);

                pipe_t *const old_pipe = existing_outpipe->pipe;

                erase_out_pipe(old_pipe);
                old_pipe->set_router_socket_routing_id(new_routing_id);
                add_out_pipe(ZMQ_MOVE(new_routing_id), old_pipe);

                if (old_pipe == _current_in)
                    _terminate_current_in = true;
                else
                    old_pipe->terminate(true);
            }
        }
    }

    pipe_->set_router_socket_routing_id(routing_id);
    add_out_pipe(ZMQ_MOVE(routing_id), pipe_); // 设置 pipe 和 routing_id 的映射关系（发送的时候会通过 routing_id 查找对应的 pipe，并发送消息）

    return true;
}
