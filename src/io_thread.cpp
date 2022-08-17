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

#include <new>

#include "macros.hpp"
#include "io_thread.hpp"
#include "err.hpp"
#include "ctx.hpp"

zmq::io_thread_t::io_thread_t(ctx_t *ctx_, uint32_t tid_)
    : object_t(ctx_, tid_)
    , _mailbox_handle(static_cast<poller_t::handle_t>(NULL))
{
    _poller = new (std::nothrow) poller_t(*ctx_);
    alloc_assert(_poller);

    if (_mailbox.get_fd() != retired_fd)
    {
        // 将 IO 线程的 mailbox 的句柄加入到 poller 的内核监听队列中。主线程向 IO 线程发送命令，会触发 IO 线程的可读事件 in_event()
        /**
         * add_fd 的参数说明:
         * 1. 第一个参数是要监听的文件描述符 fd。表面上这里是 mailbox_fd，实际上是 mailbox 内部的 eventfd. poller 监听的也是该 eventfd。socket 线程向 listen/session 发送命令的实质是向该 eventfd 写入一个字符，然后触发 eventfd 的 EPOLLIN 事件，IO 线程从 epoll::loop 开始执行 EPOLLIN 的事件处理流程
         *
         * 2. 第二个参数是该文件描述符所属的对象。当 fd 的事件触发之后，epoll 会通过该参数找到需要执行回调函数的对象.
         */
        _mailbox_handle = _poller->add_fd(_mailbox.get_fd(), this); // 这一步只是将 fd 加入到内核监听队列，并没有设置该 fd 的监听事件
        _poller->set_pollin(_mailbox_handle);       // 在这里才设置 fd 的监听事件: EPOLLIN
    }
}

zmq::io_thread_t::~io_thread_t()
{
    LIBZMQ_DELETE(_poller);
}

void zmq::io_thread_t::start()
{
    char name[16] = "";
    snprintf(name, sizeof(name), "IO/%u", get_tid() - zmq::ctx_t::reaper_tid - 1);
    //  Start the underlying I/O thread.
    // worker_poller_base_t::start
    _poller->start(name);
}

void zmq::io_thread_t::stop()
{
    send_stop();
}

zmq::mailbox_t *zmq::io_thread_t::get_mailbox()
{
    return &_mailbox;
}

int zmq::io_thread_t::get_load() const
{
    return _poller->get_load();
}

void zmq::io_thread_t::in_event()
{
    //  TODO: Do we want to limit number of commands I/O thread can
    //  process in a single go?

    command_t cmd;
    // 从 mailbox 中读取命令，并处理 process_command
    int rc = _mailbox.recv(&cmd, 0);

    // 触发了 EPOLLIN 事件时，一直读取数据，直到队列为空（是否需要限制 IO 线程在一次 EPOLLIN 事件中处理命令的数量）
    while (rc == 0 || errno == EINTR)
    {
        if (rc == 0)
            cmd.destination->process_command(cmd);
        rc = _mailbox.recv(&cmd, 0);
    }

    errno_assert(rc != 0 && errno == EAGAIN);
}

void zmq::io_thread_t::out_event()
{
    //  We are never polling for POLLOUT here. This function is never called.
    zmq_assert(false);
}

void zmq::io_thread_t::timer_event(int)
{
    //  No timers here. This function is never called.
    zmq_assert(false);
}

zmq::poller_t *zmq::io_thread_t::get_poller() const
{
    zmq_assert(_poller);
    return _poller;
}

void zmq::io_thread_t::process_stop()
{
    zmq_assert(_mailbox_handle);
    _poller->rm_fd(_mailbox_handle);
    _poller->stop();
}
