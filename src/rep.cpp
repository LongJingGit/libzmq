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
#include "rep.hpp"
#include "err.hpp"
#include "msg.hpp"

zmq::rep_t::rep_t(class ctx_t *parent_, uint32_t tid_, int sid_)
    : router_t(parent_, tid_, sid_)
    , _sending_reply(false)
    , _request_begins(true)
{
    options.type = ZMQ_REP;
}

zmq::rep_t::~rep_t() {}

int zmq::rep_t::xsend(msg_t *msg_)
{
    //  If we are in the middle of receiving a request, we cannot send reply.
    if (!_sending_reply)
    {
        errno = EFSM;
        return -1;
    }

    const bool more = (msg_->flags() & msg_t::more) != 0;

    //  Push message to the reply pipe.
    const int rc = router_t::xsend(msg_);
    if (rc != 0)
        return rc;

    //  If the reply is complete flip the FSM back to request receiving state.
    if (!more)
        _sending_reply = false;

    return 0;
}

/**
 * REP在收到消息时会将第一个空帧之前的所有信息保存起来，将原始信息传送给应用程序。
 * 在发送消息时，REP会用刚才保存的信息包裹应答消息。
 * REP其实是建立在ROUTER之上的，但和REQ一样，必须完成接受和发送这两个动作后才能继续。
 */

int zmq::rep_t::xrecv(msg_t *msg_)
{
    //  If we are in middle of sending a reply, we cannot receive next request.
    if (_sending_reply)
    {
        errno = EFSM;
        return -1;
    }

    //  First thing to do when receiving a request is to copy all the labels
    //  to the reply pipe.
    if (_request_begins)
    {
        // 在这里不断的读取带有 UUID 的消息和空消息，并且将这些消息直接通过 _current_out 发送给对端(可能是 req 或者 router)
        while (true)
        {
            int rc = router_t::xrecv(msg_);
            if (rc != 0)
                return rc;

            if ((msg_->flags() & msg_t::more))
            {
                //  Empty message part delimits the traceback stack.
                const bool bottom = (msg_->size() == 0);

                //  Push it to the reply pipe.
                rc = router_t::xsend(msg_);         // 将带有 UUID 的消息和空消息直接发送出去
                errno_assert(rc == 0);

                if (bottom)
                    break;
            }
            else
            {
                //  If the traceback stack is malformed, discard anything
                //  already sent to pipe (we're at end of invalid message).
                rc = router_t::rollback();
                errno_assert(rc == 0);
            }
        }
        _request_begins = false;
    }

    //  Get next message part to return to the user.
    const int rc = router_t::xrecv(msg_);       // 这里会读取真正的用户消息，将原始的数据返回给应用程序
    if (rc != 0)
        return rc;

    //  If whole request is read, flip the FSM to reply-sending state.
    if (!(msg_->flags() & msg_t::more))
    {
        _sending_reply = true;
        _request_begins = true;
    }

    return 0;
}

bool zmq::rep_t::xhas_in()
{
    if (_sending_reply)
        return false;

    return router_t::xhas_in();
}

bool zmq::rep_t::xhas_out()
{
    if (!_sending_reply)
        return false;

    return router_t::xhas_out();
}
