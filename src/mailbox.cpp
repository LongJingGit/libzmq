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
#include "mailbox.hpp"
#include "err.hpp"

zmq::mailbox_t::mailbox_t()
{
    //  Get the pipe into passive state. That way, if the users starts by
    //  polling on the associated file descriptor it will get woken up when
    //  new command is posted.
    const bool ok = _cpipe.check_read();
    zmq_assert(!ok);
    _active = false;
}

zmq::mailbox_t::~mailbox_t()
{
    //  TODO: Retrieve and deallocate commands inside the _cpipe.

    // Work around problem that other threads might still be in our
    // send() method, by waiting on the mutex before disappearing.
    _sync.lock();
    _sync.unlock();
}

zmq::fd_t zmq::mailbox_t::get_fd() const
{
    return _signaler.get_fd();
}

/**
 * mailbox 之间通过 signaler 进行通信
 * signaler 可以视为一个 eventfd：send 是向 eventfd 写入一个字符；recv 是从 eventfd 读取一个字符。通过这种方式可以达到两个线程通信的目的
 */
void zmq::mailbox_t::send(const command_t &cmd_)
{
    _sync.lock();
    _cpipe.write(cmd_, false);      // 将命令写入到无锁队列中
    const bool ok = _cpipe.flush(); // 手动刷新无锁队列缓存
    _sync.unlock();
    if (!ok)
        _signaler.send(); // 向 signaler 写入一个字符，作为通知消息
}

int zmq::mailbox_t::recv(command_t *cmd_, int timeout_)
{
    //  Try to get the command straight away.
    if (_active)
    {
        if (_cpipe.read(cmd_))
            return 0;

        //  If there are no more commands available, switch into passive state.
        _active = false;
    }

    /**
     * 如果 mailbox_t::recv 接口是从 io_thread_t::in_event 中调用过来的，那么说明 _signaler 中的 eventfd 的可读事件已经被触发了.
     * 在 _signaler.wait 接口中重新监听 eventfd 的可读事件（如果 eventfd 没有被读取，则可读事件会在这里再次触发）,
     * 然后在 _signaler.recv_failable 接口中读取 eventfd 中的数据，最后调用 _cpipe.read 读取 command
     */

    //  Wait for signal from the command sender.
    int rc = _signaler.wait(timeout_);
    if (rc == -1)
    {
        errno_assert(errno == EAGAIN || errno == EINTR);
        return -1;
    }

    //  Receive the signal.
    rc = _signaler.recv_failable();
    if (rc == -1)
    {
        errno_assert(errno == EAGAIN);
        return -1;
    }

    //  Switch into active state.
    _active = true;

    //  Get a command.
    const bool ok = _cpipe.read(cmd_);
    zmq_assert(ok);
    return 0;
}

bool zmq::mailbox_t::valid() const
{
    return _signaler.valid();
}
