
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

#ifndef __ZMQ_YPIPE_BASE_HPP_INCLUDED__
#define __ZMQ_YPIPE_BASE_HPP_INCLUDED__

#include "macros.hpp"

namespace zmq {
// ypipe_base abstracts ypipe and ypipe_conflate specific
// classes, one is selected according to a the conflate
// socket option

template<typename T>
class ypipe_base_t
{
public:
    virtual ~ypipe_base_t() ZMQ_DEFAULT;
    virtual void write(const T &value_, bool incomplete_) = 0;
    virtual bool unwrite(T *value_) = 0;
    virtual bool flush() = 0;
    virtual bool check_read() = 0;
    virtual bool read(T *value_) = 0;
    virtual bool probe(bool (*fn_)(const T &)) = 0;
};
} // namespace zmq

#endif
