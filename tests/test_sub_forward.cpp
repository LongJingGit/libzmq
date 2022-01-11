/*
    Copyright (c) 2007-2017 Contributors as noted in the AUTHORS file

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

#include "testutil.hpp"
#include "testutil_unity.hpp"

SETUP_TEARDOWN_TESTCONTEXT

/**
 * @brief 发布-订阅模型（单向的数据分发）
 *
 *              ZMQ_PUB         发布
 *                 |
 *              ZMQ_XSUB        中间件：订阅消息
 *
 *              ZMQ_XPUB        中间件：发布消息
 *                 |
 *              ZMQ_SUB         订阅
 *
 * SUB 需要订阅哪些消息，可以通过 zmq_setsockopt 设置，在 zmq_setsockopt 调用内部，会构造订阅消息，告知 XPUB 要订阅哪些消息，XPUB 接收到 SUB 发送过来的订阅消息之后，会订阅需要订阅的消息；
 *
 * XSUB 和 XPUB 之间并没有实际的数据交换通道，数据是通过应用层传递的（XSUB 接收到消息，然后交给 XPUB 发送），也就是说，XSUB 和 XPUB 之间并不会过滤任何消息；
 *
 * XPUB 转发 XSUB 接收到的数据，在发送数据的时候，会在内部过滤消息，只发送 SUB 订阅的消息给 SUB
 *
 */
void test ()
{
    char endpoint1[MAX_SOCKET_STRING];
    char endpoint2[MAX_SOCKET_STRING];

    //  First, create an intermediate device
    void *xpub = test_context_socket (ZMQ_XPUB);
    bind_loopback_ipv4 (xpub, endpoint1, sizeof (endpoint1));

    void *xsub = test_context_socket (ZMQ_XSUB);
    bind_loopback_ipv4 (xsub, endpoint2, sizeof (endpoint2));

    //  Create a publisher
    void *pub = test_context_socket (ZMQ_PUB);
    TEST_ASSERT_SUCCESS_ERRNO (zmq_connect (pub, endpoint2));

    //  Create a subscriber
    void *sub = test_context_socket (ZMQ_SUB);
    TEST_ASSERT_SUCCESS_ERRNO (zmq_connect (sub, endpoint1));

    //  Subscribe for all messages.
    /**
     * 在 setsockopt 里面，sub 会构造一个订阅消息(被初始化成 ZMQ_SUBSCRIBE)，将需要订阅的消息列表保存到 _subscriptions 中；
     * 同时，sub 会将该订阅消息发送给 XPUB，XPUB 接收到该订阅消息后，会将需要订阅的消息保存在 _subscriptions 参数中（xpub 会订阅 和 sub 相同的消息）
     */
    TEST_ASSERT_SUCCESS_ERRNO (zmq_setsockopt (sub, ZMQ_SUBSCRIBE, "", 0));

    //  Pass the subscription upstream through the device
    char buff[32];
    int size;
    // XPUB 会接收来自 SUB 的订阅消息，将需要订阅的消息保存在 _subscriptions 参数中
    // 实际上，在调用 zmq_recv 接收消息内部，会先读取 mailbox 的命令，处理其他模块发送过来的命令（在处理命令的时候会更新订阅消息列表）
    // buff 中消息内容为："1"
    TEST_ASSERT_SUCCESS_ERRNO (size = zmq_recv (xpub, buff, sizeof (buff), 0));
    /**
     * @brief xsub 转发订阅消息
     * 这里并不是真正的将消息转发出去，而是让 xsub 在 send 的时候解析订阅消息，设置自己内部的 _subscriptions 参数，从而能够订阅和 XPUB 相同的消息。
     * 否则 xsub 无法订阅消息
     */
    TEST_ASSERT_SUCCESS_ERRNO (zmq_send (xsub, buff, size, 0));

    //  Wait a bit till the subscription gets to the publisher
    msleep (SETTLE_TIME);

    //  Send an empty message
    send_string_expect_success (pub, "", 0);            // 发布消息（不会过滤消息）

    //  Pass the message downstream through the device
    TEST_ASSERT_SUCCESS_ERRNO (size = zmq_recv (xsub, buff, sizeof (buff), 0));     // xsub 订阅消息（会过滤消息：只接收订阅的消息）
    TEST_ASSERT_SUCCESS_ERRNO (zmq_send (xpub, buff, size, 0));                     // xpub 转发接收到的消息（过滤消息：只发送订阅的消息）

    //  Receive the message in the subscriber
    recv_string_expect_success (sub, "", 0);                                        // sub 订阅消息

    //  Clean up.
    test_context_socket_close (xpub);
    test_context_socket_close (xsub);
    test_context_socket_close (pub);
    test_context_socket_close (sub);
}

int main ()
{
    setup_test_environment ();

    UNITY_BEGIN ();
    RUN_TEST (test);
    return UNITY_END ();
}
