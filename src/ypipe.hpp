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

#ifndef __ZMQ_YPIPE_HPP_INCLUDED__
#define __ZMQ_YPIPE_HPP_INCLUDED__

#include "atomic_ptr.hpp"
#include "yqueue.hpp"
#include "ypipe_base.hpp"

namespace zmq {
//  Lock-free queue implementation.
//  Only a single thread can read from the pipe at any specific moment.
//  Only a single thread can write to the pipe at any specific moment.
//  T is the type of the object in the queue.
//  N is granularity of the pipe, i.e. how many items are needed to
//  perform next memory allocation.

template<typename T, int N>
class ypipe_t ZMQ_FINAL : public ypipe_base_t<T>
{
public:
    //  Initialises the pipe.
    ypipe_t()
    {
        //  Insert terminator element into the queue.
        _queue.push();

        //  Let all the pointers to point to the terminator.
        //  (unless pipe is dead, in which case c is set to NULL).
        _r = _w = _f = &_queue.back();
        _c.set(&_queue.back());
    }

    //  Following function (write) deliberately copies uninitialised data
    //  when used with zmq_msg. Initialising the VSM body for
    //  non-VSM messages won't be good for performance.

#ifdef ZMQ_HAVE_OPENVMS
#    pragma message save
#    pragma message disable(UNINIT)
#endif

    //  Write an item to the pipe.  Don't flush it yet. If incomplete is
    //  set to true the item is assumed to be continued by items
    //  subsequently written to the pipe. Incomplete items are never
    //  flushed down the stream.
    // 注意：虽然调用了 write 操作将消息写入到了队列中，但是因为没有调用 flush, 没有更新读写索引，所以对端此时读取不到这条消息
    void write(const T &value_, bool incomplete_)
    {
        //  Place the value to the queue, add new terminator element.
        _queue.back() = value_; // 注意：这里存放的不是指向消息的指针，而是真实的消息本身(back 返回值的是 T, 这里执行消息的拷贝赋值)
        _queue.push();

        //  Move the "flush up to here" poiter.
        // 置 flush 指针，标明 flush 的位置
        if (!incomplete_)
            _f = &_queue.back(); // 只有发送完最后一帧消息，才会置 _f，对端的 check_read 才可以返回 true，对端此时才可以从队列头部读取数据
    }

#ifdef ZMQ_HAVE_OPENVMS
#    pragma message restore
#endif

    //  Pop an incomplete item from the pipe. Returns true if such
    //  item exists, false otherwise.
    bool unwrite(T *value_)
    {
        if (_f == &_queue.back())
            return false;
        _queue.unpush();
        *value_ = _queue.back();
        return true;
    }

    //  Flush all the completed items into the pipe. Returns false if
    //  the reader thread is sleeping. In that case, caller is obliged to
    //  wake the reader up before using the pipe again.
    // 只有执行 flush 的时候才会更新读写索引
    bool flush()
    {
        //  If there are no un-flushed items, do nothing.
        if (_w == _f)
            return true;

        //  Try to set 'c' to 'f'.
        if (_c.cas(_w, _f) != _w) // CAS 操作：如果 _c 和 _w 相等，则会将 _c 替换为 _f 的值，返回替换之前的值；否则将 _w 更新为 _c
        {
            //  Compare-and-swap was unseccessful because 'c' is NULL.
            //  This means that the reader is asleep. Therefore we don't
            //  care about thread-safeness and update c in non-atomic
            //  manner. We'll return false to let the caller know
            //  that reader is sleeping.
            _c.set(_f);
            _w = _f;
            return false;
        }

        //  Reader is alive. Nothing special to do now. Just move
        //  the 'first un-flushed item' pointer to 'f'.
        _w = _f;
        return true;
    }

    //  Check whether item is available for reading.
    bool check_read()
    {
        //  Was the value prefetched already? If so, return.
        if (&_queue.front() != _r && _r)
            return true;

        //  There's no prefetched value, so let us prefetch more values.
        //  Prefetching is to simply retrieve the
        //  pointer from c in atomic fashion. If there are no
        //  items to prefetch, set c to NULL (using compare-and-swap).
        _r = _c.cas(&_queue.front(), NULL);

        //  If there are no elements prefetched, exit.
        //  During pipe's lifetime r should never be NULL, however,
        //  it can happen during pipe shutdown when items
        //  are being deallocated.
        if (&_queue.front() == _r || !_r)
            return false;

        //  There was at least one value prefetched.
        return true;
    }

    //  Reads an item from the pipe. Returns false if there is no value.
    //  available.
    bool read(T *value_)
    {
        //  Try to prefetch a value.
        if (!check_read())
            return false;

        //  There was at least one value prefetched.
        //  Return it to the caller.
        *value_ = _queue.front(); // 从队列头部取消息（这里会执行一次 T 的拷贝赋值?）
        _queue.pop();             // 移动 begin_pos, 如果一个 chunk 的消息全部读取完毕，则队列会释放掉该 chunk 的内存空间
        return true;
    }

    //  Applies the function fn to the first elemenent in the pipe
    //  and returns the value returned by the fn.
    //  The pipe mustn't be empty or the function crashes.
    bool probe(bool (*fn_)(const T &))
    {
        const bool rc = check_read();
        zmq_assert(rc);

        return (*fn_)(_queue.front());
    }

protected:
    //  Allocation-efficient queue to store pipe items.
    //  Front of the queue points to the first prefetched item, back of
    //  the pipe points to last un-flushed item. Front is used only by
    //  reader thread, while back is used only by writer thread.
    /**
     * 普通的无锁队列的实现: 底层是一段提前分配好的连续的内存，入队操作使用 placement new 构造对象。典型代表: ConcurrentQueue, folly/ProducerConsumerQueue.h
     *
     * ZeroMQ 底层的无锁队列提前构造好了 N 个对象 T, 入队操作仅仅需要移动下标即可(还需要执行 T 的拷贝赋值)
     */
    yqueue_t<T, N> _queue;

    //  Points to the first un-flushed item. This variable is used
    //  exclusively by writer thread.
    T *_w;

    //  Points to the first un-prefetched item. This variable is used
    //  exclusively by reader thread.
    T *_r;

    //  Points to the first item to be flushed in the future.
    T *_f;

    //  The single point of contention between writer and reader thread.
    //  Points past the last flushed item. If it is NULL,
    //  reader is asleep. This pointer should be always accessed using
    //  atomic operations.
    atomic_ptr_t<T> _c;

    ZMQ_NON_COPYABLE_NOR_MOVABLE(ypipe_t)
};
} // namespace zmq

#endif
