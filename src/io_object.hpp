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

#ifndef __ZMQ_IO_OBJECT_HPP_INCLUDED__
#define __ZMQ_IO_OBJECT_HPP_INCLUDED__

#include <stddef.h>

#include "stdint.hpp"
#include "poller.hpp"
#include "i_poll_events.hpp"

namespace zmq
{
class io_thread_t;

//  Simple base class for objects that live in I/O threads.
//  It makes communication with the poller object easier and
//  makes defining unneeded event handlers unnecessary.

class io_object_t : public i_poll_events
{
  public:
    io_object_t (zmq::io_thread_t *io_thread_ = NULL);
    ~io_object_t () ZMQ_OVERRIDE;

    //  When migrating an object from one I/O thread to another, first
    //  unplug it, then migrate it, then plug it to the new thread.
    void plug (zmq::io_thread_t *io_thread_);
    void unplug ();

  protected:
    typedef poller_t::handle_t handle_t;

    //  Methods to access underlying poller object.
    handle_t add_fd (fd_t fd_);
    void rm_fd (handle_t handle_);
    void set_pollin (handle_t handle_);
    void reset_pollin (handle_t handle_);
    void set_pollout (handle_t handle_);
    void reset_pollout (handle_t handle_);
    void add_timer (int timeout_, int id_);
    void cancel_timer (int id_);

    //  i_poll_events interface implementation.
    void in_event () ZMQ_OVERRIDE;
    void out_event () ZMQ_OVERRIDE;
    void timer_event (int id_) ZMQ_OVERRIDE;

  private:
  /**
   * io_object 可以获取 io_thread 的 poller，从而具备 poller 的功能。
   * 所有继承于该类的子类都具有 poller 的功能，可监听句柄的读写异常状态
   */
    poller_t *_poller;      // 在实例化 io_object_t 的时候，就会调用 ::plug 接口获取 _poller 指针

    ZMQ_NON_COPYABLE_NOR_MOVABLE (io_object_t)
};
}

#endif
