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
#include "push.hpp"
#include "pipe.hpp"
#include "err.hpp"
#include "msg.hpp"

zmq::push_t::push_t (class ctx_t *parent_, uint32_t tid_, int sid_) :
    socket_base_t (parent_, tid_, sid_)
{
    options.type = ZMQ_PUSH;
}

zmq::push_t::~push_t ()
{
}

/**
 * 如果设置了 ZMQ_IMMEDIATE 标志，会在 zmq_connnect 的时候就会创建 pipe 并同时 attach pipe;
 * 如果没有设置该标志，则会在连接建立完成之后 engine_ready 中创建 pipe，并发送命令给 socket，socket 在调用 recv 接收消息的时候首先处理 bind 命令，然后 attach pipe。
 *
 * PUSH 使用负载均衡的方式向每一个连接的 socket 均衡发送消息的。如果设置了 ZMQ_IMMEDIATE，则如果连接到无效的 socket 的时候，发送给该 socket 的消息就会被丢弃；
 * 但是，如果没有设置该标志，则 attach pipe 只会 attach 有效的 socket 的 pipe，从 socket 发送的消息，则所有消息都会发送给连接成功的有效 socket。不会出现消息丢失的情况。
 */
void zmq::push_t::xattach_pipe (pipe_t *pipe_,
                                bool subscribe_to_all_,
                                bool locally_initiated_)
{
    LIBZMQ_UNUSED (subscribe_to_all_);
    LIBZMQ_UNUSED (locally_initiated_);

    //  Don't delay pipe termination as there is no one
    //  to receive the delimiter.
    pipe_->set_nodelay ();

    zmq_assert (pipe_);
    _lb.attach (pipe_);
}

void zmq::push_t::xwrite_activated (pipe_t *pipe_)
{
    _lb.activated (pipe_);
}

void zmq::push_t::xpipe_terminated (pipe_t *pipe_)
{
    _lb.pipe_terminated (pipe_);
}

int zmq::push_t::xsend (msg_t *msg_)
{
    return _lb.send (msg_);
}

bool zmq::push_t::xhas_out ()
{
    return _lb.has_out ();
}
