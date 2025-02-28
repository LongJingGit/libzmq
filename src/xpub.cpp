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
#include <string.h>

#include "xpub.hpp"
#include "pipe.hpp"
#include "err.hpp"
#include "msg.hpp"
#include "macros.hpp"
#include "generic_mtrie_impl.hpp"

zmq::xpub_t::xpub_t(class ctx_t *parent_, uint32_t tid_, int sid_)
    : socket_base_t(parent_, tid_, sid_)
    , _verbose_subs(false)
    , _verbose_unsubs(false)
    , _more_send(false)
    , _more_recv(false)
    , _process_subscribe(false)
    , _only_first_subscribe(false)
    , _lossy(true)
    , _manual(false)
    , _send_last_pipe(false)
    , _pending_pipes()
    , _welcome_msg()
{
    _last_pipe = NULL;
    options.type = ZMQ_XPUB;
    _welcome_msg.init();
}

zmq::xpub_t::~xpub_t()
{
    _welcome_msg.close();
    for (std::deque<metadata_t *>::iterator it = _pending_metadata.begin(), end = _pending_metadata.end(); it != end; ++it)
        if (*it && (*it)->drop_ref())
            LIBZMQ_DELETE(*it);
}

// 绑定 pipe (有可能是对端发送了 bind 命令之后，本 socket 在 send/recv 之前先处理 pending 命令，然后执行 process_bind 命令)
void zmq::xpub_t::xattach_pipe(pipe_t *pipe_, bool subscribe_to_all_, bool locally_initiated_)
{
    LIBZMQ_UNUSED(locally_initiated_);

    zmq_assert(pipe_);
    _dist.attach(pipe_);    // 在这里会更新 dist 的 active pipe 数量. 发送和接收消息都是通过 dist 的 pipe 进行的

    //  If subscribe_to_all_ is specified, the caller would like to subscribe
    //  to all data on this pipe, implicitly.
    if (subscribe_to_all_)
        _subscriptions.add(NULL, 0, pipe_);

    // if welcome message exists, send a copy of it
    if (_welcome_msg.size() > 0)
    {
        msg_t copy;
        copy.init();
        const int rc = copy.copy(_welcome_msg);
        errno_assert(rc == 0);
        const bool ok = pipe_->write(&copy);
        zmq_assert(ok);
        pipe_->flush();
    }

    //  The pipe is active when attached. Let's read the subscriptions from
    //  it, if any.
    // 注意: 这里只可能读取到的是订阅消息, 不可能是其他消息（pub socket）
    xread_activated(pipe_);
}

void zmq::xpub_t::xread_activated(pipe_t *pipe_)
{
    //  There are some subscriptions waiting. Let's process them.
    msg_t msg;
    while (pipe_->read(&msg))
    {
        metadata_t *metadata = msg.metadata();
        unsigned char *msg_data = static_cast<unsigned char *>(msg.data()), *data = NULL;
        size_t size = 0;
        bool subscribe = false;
        bool is_subscribe_or_cancel = false;
        bool notify = false;

        const bool first_part = !_more_recv;
        _more_recv = (msg.flags() & msg_t::more) != 0;

        if (first_part || _process_subscribe)
        {
            //  Apply the subscription to the trie
            if (msg.is_subscribe() || msg.is_cancel())
            {
                data = static_cast<unsigned char *>(msg.command_body());
                size = msg.command_body_size();
                subscribe = msg.is_subscribe();
                is_subscribe_or_cancel = true;
            }
            // 判断是否是新增订阅/删除订阅消息. 新增订阅: 第一个字节为 1; 删除订阅: 第一个字节为 0
            else if (msg.size() > 0 && (*msg_data == 0 || *msg_data == 1))
            {
                data = msg_data + 1;        // 要删除或者新增的消息类型
                size = msg.size() - 1;
                subscribe = *msg_data == 1;
                is_subscribe_or_cancel = true;
            }
        }

        if (first_part)
            _process_subscribe = !_only_first_subscribe || is_subscribe_or_cancel;

        if (is_subscribe_or_cancel)
        {
            /**
             * 1. socket 设置了 ZMQ_XPUB_MANUAL 属性, 则 _manual 为 true, 将订阅消息类型和 pipe 的映射保存到 _manual_subscriptions 中
             * 2. socket 没有设置 ZMQ_XPUB_MANUAL 属性，将订阅消息类型和 pipe 的映射保存到 _subscriptions 中
             */
            if (_manual)
            {
                // Store manual subscription to use on termination
                if (!subscribe)
                    _manual_subscriptions.rm(data, size, pipe_);        // 删除订阅
                else
                    _manual_subscriptions.add(data, size, pipe_); // 新增订阅: 保存新增订阅类型和 pipe_ 的映射到手动订阅列表中

                _pending_pipes.push_back(pipe_);        // 保存输出 pipe
            }
            else
            {
                if (!subscribe)
                {
                    const mtrie_t::rm_result rm_result = _subscriptions.rm(data, size, pipe_);
                    //  TODO reconsider what to do if rm_result == mtrie_t::not_found
                    notify = rm_result != mtrie_t::values_remain || _verbose_unsubs;
                }
                else
                {
                    // 更新 pipe_ 和订阅消息类型的映射关系。通过 pipe_ 发送和接收消息会根据这个映射关系进行过滤
                    const bool first_added = _subscriptions.add(data, size, pipe_);
                    notify = first_added || _verbose_subs;
                }
            }

            //  If the request was a new subscription, or the subscription
            //  was removed, or verbose mode or manual mode are enabled, store it
            //  so that it can be passed to the user on next recv call.
            /**
             * 如果请求是新订阅, 或已删除订阅, 或者 socket 设置了 ZMQ_XPUB_MANUAL/ZMQ_XPUB_VERBOSE 属性,
             * 则将其存储起来，以便在下一次 recv 调用时将其传递给用户。
             */
            if (_manual || (options.type == ZMQ_XPUB && notify))
            {
                //  ZMTP 3.1 hack: we need to support sub/cancel commands, but
                //  we can't give them back to userspace as it would be an API
                //  breakage since the payload of the message is completely
                //  different. Manually craft an old-style message instead.
                //  Although with other transports it would be possible to simply
                //  reuse the same buffer and prefix a 0/1 byte to the topic, with
                //  inproc the subscribe/cancel command string is not present in
                //  the message, so this optimization is not possible.
                //  The pushback makes a copy of the data array anyway, so the
                //  number of buffer copies does not change.
                blob_t notification(size + 1);
                if (subscribe)
                    *notification.data() = 1;
                else
                    *notification.data() = 0;
                memcpy(notification.data() + 1, data, size);    // 构造一个新增订阅/删除订阅的消息

                _pending_data.push_back(ZMQ_MOVE(notification)); // 将构造的新消息保存起来. 调用 xpub_t::xrecv 时会将该消息返回给应用层
                if (metadata)
                    metadata->add_ref();
                _pending_metadata.push_back(metadata);
                _pending_flags.push_back(0);
            }
        }
        else if (options.type != ZMQ_PUB)
        {
            //  Process user message coming upstream from xsub socket,
            //  but not if the type is PUB, which never processes user
            //  messages
            _pending_data.push_back(blob_t(msg_data, msg.size()));
            if (metadata)
                metadata->add_ref();
            _pending_metadata.push_back(metadata);
            _pending_flags.push_back(msg.flags());
        }

        msg.close();
    }
}

void zmq::xpub_t::xwrite_activated(pipe_t *pipe_)
{
    _dist.activated(pipe_);
}

int zmq::xpub_t::xsetsockopt(int option_, const void *optval_, size_t optvallen_)
{
    // clang-format off
    if (option_ == ZMQ_XPUB_VERBOSE
     || option_ == ZMQ_XPUB_VERBOSER
     || option_ == ZMQ_XPUB_MANUAL_LAST_VALUE
     || option_ == ZMQ_XPUB_NODROP
     || option_ == ZMQ_XPUB_MANUAL
     || option_ == ZMQ_ONLY_FIRST_SUBSCRIBE)
    // clang-format on
    {
        if (optvallen_ != sizeof(int) || *static_cast<const int *>(optval_) < 0)
        {
            errno = EINVAL;
            return -1;
        }
        if (option_ == ZMQ_XPUB_VERBOSE)
        {
            _verbose_subs = (*static_cast<const int *>(optval_) != 0);
            _verbose_unsubs = false;
        }
        else if (option_ == ZMQ_XPUB_VERBOSER)
        {
            _verbose_subs = (*static_cast<const int *>(optval_) != 0);
            _verbose_unsubs = _verbose_subs;
        }
        else if (option_ == ZMQ_XPUB_MANUAL_LAST_VALUE)
        {
            _manual = (*static_cast<const int *>(optval_) != 0);
            _send_last_pipe = _manual;
        }
        else if (option_ == ZMQ_XPUB_NODROP)
            _lossy = (*static_cast<const int *>(optval_) == 0);
        // 手动管理订阅消息的 socket 选项
        else if (option_ == ZMQ_XPUB_MANUAL)
            _manual = (*static_cast<const int *>(optval_) != 0);
        else if (option_ == ZMQ_ONLY_FIRST_SUBSCRIBE)
            _only_first_subscribe = (*static_cast<const int *>(optval_) != 0);
    }
    // 如果用户想要动态的订阅和取消订阅某种类型的消息，需要 ZMQ_XPUB_MANUAL 和 ZMQ_SUBSCRIBE/ZMQ_UNSUBSCRIBE 一起使用
    else if (option_ == ZMQ_SUBSCRIBE && _manual)
    {
        if (_last_pipe != NULL)
            _subscriptions.add((unsigned char *)optval_, optvallen_, _last_pipe);       // 保存 pipe 和订阅类型的映射关系
    }
    else if (option_ == ZMQ_UNSUBSCRIBE && _manual)
    {
        if (_last_pipe != NULL)
            _subscriptions.rm((unsigned char *)optval_, optvallen_, _last_pipe);
    }
    else if (option_ == ZMQ_XPUB_WELCOME_MSG)
    {
        _welcome_msg.close();

        if (optvallen_ > 0)
        {
            const int rc = _welcome_msg.init_size(optvallen_);
            errno_assert(rc == 0);

            unsigned char *data = static_cast<unsigned char *>(_welcome_msg.data());
            memcpy(data, optval_, optvallen_);
        }
        else
            _welcome_msg.init();
    }
    else
    {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

static void stub(zmq::mtrie_t::prefix_t data_, size_t size_, void *arg_)
{
    LIBZMQ_UNUSED(data_);
    LIBZMQ_UNUSED(size_);
    LIBZMQ_UNUSED(arg_);
}

void zmq::xpub_t::xpipe_terminated(pipe_t *pipe_)
{
    if (_manual)
    {
        //  Remove the pipe from the trie and send corresponding manual
        //  unsubscriptions upstream.
        _manual_subscriptions.rm(pipe_, send_unsubscription, this, false);
        //  Remove pipe without actually sending the message as it was taken
        //  care of by the manual call above. subscriptions is the real mtrie,
        //  so the pipe must be removed from there or it will be left over.
        _subscriptions.rm(pipe_, stub, static_cast<void *>(NULL), false);
    }
    else
    {
        //  Remove the pipe from the trie. If there are topics that nobody
        //  is interested in anymore, send corresponding unsubscriptions
        //  upstream.
        _subscriptions.rm(pipe_, send_unsubscription, this, !_verbose_unsubs);
    }

    _dist.pipe_terminated(pipe_);
}

void zmq::xpub_t::mark_as_matching(pipe_t *pipe_, xpub_t *self_)
{
    self_->_dist.match(pipe_);
}

void zmq::xpub_t::mark_last_pipe_as_matching(pipe_t *pipe_, xpub_t *self_)
{
    if (self_->_last_pipe == pipe_)
        self_->_dist.match(pipe_);
}

int zmq::xpub_t::xsend(msg_t *msg_)
{
    const bool msg_more = (msg_->flags() & msg_t::more) != 0;

    //  For the first part of multi-part message, find the matching pipes.
    if (!_more_send)
    {
        // Ensure nothing from previous failed attempt to send is left matched
        _dist.unmatch();

        // _subscriptions.match 判断要发送的消息 msg 和对应的发送管道 pipe 是否匹配.
        // 如果匹配的话会调用回调函数 mark_as_matching/mark_last_pipe_as_matching
        if (unlikely(_manual && _last_pipe && _send_last_pipe))
        {
            _subscriptions.match(static_cast<unsigned char *>(msg_->data()), msg_->size(), mark_last_pipe_as_matching, this);
            _last_pipe = NULL;
        }
        else
        {
            _subscriptions.match(static_cast<unsigned char *>(msg_->data()), msg_->size(), mark_as_matching, this);
        }

        // If inverted matching is used, reverse the selection now
        if (options.invert_matching)
        {
            _dist.reverse_match();
        }
    }

    int rc = -1; //  Assume we fail
    if (_lossy || _dist.check_hwm())            // 检查 HWM（如果 check_hwm 返回失败，则不再继续发送消息，并设置错误码为 EAGAIN）
    {
        // 将 msg_ 发送给每一个匹配的 pipe
        if (_dist.send_to_matching(msg_) == 0)
        {
            //  If we are at the end of multi-part message we can mark
            //  all the pipes as non-matching.
            if (!msg_more)
                _dist.unmatch();
            _more_send = msg_more;
            rc = 0; //  Yay, sent successfully
        }
    }
    else
        errno = EAGAIN;
    return rc;
}

bool zmq::xpub_t::xhas_out()
{
    return _dist.has_out();
}

// 一般情况下，不能使用 pub 类套接字接收消息（但是订阅消息除外）
int zmq::xpub_t::xrecv(msg_t *msg_)
{
    //  If there is at least one
    if (_pending_data.empty()) // 如果 socket 设置了 ZMQ_XPUB_MANUAL 属性, 则会在 xread_activated 中填充该 vector 的
    {
        errno = EAGAIN;
        return -1;
    }

    // User is reading a message, set last_pipe and remove it from the deque
    if (_manual && !_pending_pipes.empty())
    {
        _last_pipe = _pending_pipes.front();
        _pending_pipes.pop_front();
    }

    int rc = msg_->close();
    errno_assert(rc == 0);
    rc = msg_->init_size(_pending_data.front().size());
    errno_assert(rc == 0);
    memcpy(msg_->data(), _pending_data.front().data(), _pending_data.front().size()); // 用 xattach_pipe 中构造的 pending_data 来填充返回消息

    // set metadata only if there is some
    if (metadata_t *metadata = _pending_metadata.front())
    {
        msg_->set_metadata(metadata);
        // Remove ref corresponding to vector placement
        metadata->drop_ref();
    }

    msg_->set_flags(_pending_flags.front());
    _pending_data.pop_front();
    _pending_metadata.pop_front();
    _pending_flags.pop_front();
    return 0;
}

bool zmq::xpub_t::xhas_in()
{
    return !_pending_data.empty();
}

void zmq::xpub_t::send_unsubscription(zmq::mtrie_t::prefix_t data_, size_t size_, xpub_t *self_)
{
    if (self_->options.type != ZMQ_PUB)
    {
        //  Place the unsubscription to the queue of pending (un)subscriptions
        //  to be retrieved by the user later on.
        blob_t unsub(size_ + 1);
        *unsub.data() = 0;
        if (size_ > 0)
            memcpy(unsub.data() + 1, data_, size_);
        self_->_pending_data.ZMQ_PUSH_OR_EMPLACE_BACK(ZMQ_MOVE(unsub));
        self_->_pending_metadata.push_back(NULL);
        self_->_pending_flags.push_back(0);

        if (self_->_manual)
        {
            self_->_last_pipe = NULL;
            self_->_pending_pipes.push_back(NULL);
        }
    }
}
