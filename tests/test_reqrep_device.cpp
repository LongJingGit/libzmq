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
 * @brief 请求-应答代理
 *                   REQ
 *                    |
 *                    |
 *                  ROUTER
 *
 *
 *                  DEALER
 *                    |
 *                    |
 *                   REP
 *
 * ROUTER 会将消息来自哪一个 REQ 记录下来，生成一个信封。DEALER 和 REP 套接字在传输消息的过程中不会丢弃或者更改消息内容，这样当消息返回给 ROUTER 时，
 * 它就知道该发送给哪一个 REQ 了。
 *
 * 关于 router 和 信封 的理解可以参考代码和注释，并同时借鉴:
 * 《ZMQ 指南》第三章 高级请求-应答模式 (https://wizardforcel.gitbooks.io/zmq-guide/content/chapter3.html)
 */

void test_roundtrip ()
{
    char endpoint1[MAX_SOCKET_STRING];
    char endpoint2[MAX_SOCKET_STRING];

    //  Create a req/rep device.
    void *dealer = test_context_socket (ZMQ_DEALER);
    bind_loopback_ipv4 (dealer, endpoint1, sizeof (endpoint1));

    void *router = test_context_socket (ZMQ_ROUTER);
    bind_loopback_ipv4 (router, endpoint2, sizeof (endpoint2));

    //  Create a worker.
    void *rep = test_context_socket (ZMQ_REP);
    TEST_ASSERT_SUCCESS_ERRNO (zmq_connect (rep, endpoint1));     // rep 套接字连接至 dealer

    //  Create a client.
    void *req = test_context_socket (ZMQ_REQ);
    TEST_ASSERT_SUCCESS_ERRNO (zmq_connect (req, endpoint2));   // req 套接字连接至 router

    //  Send a request.
    /**
     * req 会发送三帧消息（内部队列中可验证）：
     * 1. 长度为 0 的空帧：req 使用 send 发送消息，但是在发送用户数据之前，会先发送一条长度为 0 的空消息，在 req socket 内部构造的，具体参考 req.cpp
     * 2. 用户消息："ABC"
     * 3. 用户消息："DEF"
     */
    send_string_expect_success (req, "ABC", ZMQ_SNDMORE);     // req 发送消息给 router
    send_string_expect_success (req, "DEF", 0);       // 发送多帧消息时，只有当最后一条消息提交发送了，整个消息才会被发送

    //  Pass the request through the device.
    /**
     * router 收到四帧消息：
     * 1. routing_id
     * 2. 空帧：由 REQ 套接字发送给 ROUTER 真实的用户消息之前发送的
     * 3. 用户消息: "ABC"
     * 4. 用户消息: "DEF"
     */
    for (int i = 0; i != 4; i++)
    {
        zmq_msg_t msg;
        TEST_ASSERT_SUCCESS_ERRNO (zmq_msg_init (&msg));
        TEST_ASSERT_SUCCESS_ERRNO (zmq_msg_recv (&msg, router, 0));       // router 接收消息（来自 req）
        int rcvmore;
        size_t sz = sizeof (rcvmore);
        TEST_ASSERT_SUCCESS_ERRNO (zmq_getsockopt (router, ZMQ_RCVMORE, &rcvmore, &sz));
        TEST_ASSERT_SUCCESS_ERRNO (zmq_msg_send (&msg, dealer, rcvmore ? ZMQ_SNDMORE : 0));      // dealer 发送消息给 rep
    }

    /**
     * dealer 发送了四条消息:
     * 1. routing_id
     * 2. 空帧
     * 3. ABC
     * 4. DEF
     */

    /**
     * rep 接收到五条消息:
     * 1. routing_id: 这条消息是 router 使用 routing_id 构造出来的. 该消息在 rep_t::xrecv()-->router_t::xsend() 中用来寻找发送路由，并不会实际发送给对端
     * 2. routing_id: 该消息在 rep_t::xrecv()-->router_t::xsend 中被发送给了对端, 不会递交给上层
     * 3. 空帧: 该消息在 rep_t::xrecv()-->router_t::xsend 中被发送给了对端, 不会递交给上层
     * 4. ABC
     * 5. DEF
     */

    //  Receive the request.
    recv_string_expect_success (rep, "ABC", 0);
    int rcvmore;
    size_t sz = sizeof (rcvmore);
    TEST_ASSERT_SUCCESS_ERRNO (zmq_getsockopt (rep, ZMQ_RCVMORE, &rcvmore, &sz));
    TEST_ASSERT_TRUE (rcvmore);
    recv_string_expect_success (rep, "DEF", 0);
    TEST_ASSERT_SUCCESS_ERRNO (zmq_getsockopt (rep, ZMQ_RCVMORE, &rcvmore, &sz));
    TEST_ASSERT_FALSE (rcvmore);

    /************************************************/

    /**
     * rep 发送了四条消息:
     * 1. routing_id: rep_t::xrecv()-->router_t::xsend() 中被发送给了对端
     * 2. 空帧: rep_t::xrecv()-->router_t::xsend() 中被发送给了对端
     * 3. GHI
     * 4. JKL
     */

    //  Send the reply.
    send_string_expect_success (rep, "GHI", ZMQ_SNDMORE);
    send_string_expect_success (rep, "JKL", 0);

    /**
     * dealer 收到了四条消息：
     * 1. routing_id
     * 2. 空帧
     * 3. GHI
     * 4. JKL
     */

    //  Pass the reply through the device.
    for (int i = 0; i != 4; i++)
    {
        zmq_msg_t msg;
        TEST_ASSERT_SUCCESS_ERRNO (zmq_msg_init (&msg));
        TEST_ASSERT_SUCCESS_ERRNO (zmq_msg_recv (&msg, dealer, 0));
        int rcvmore;
        size_t sz = sizeof (rcvmore);
        TEST_ASSERT_SUCCESS_ERRNO (zmq_getsockopt (dealer, ZMQ_RCVMORE, &rcvmore, &sz));
        TEST_ASSERT_SUCCESS_ERRNO (zmq_msg_send (&msg, router, rcvmore ? ZMQ_SNDMORE : 0));
    }

    /**
     * 通过 router 将 dealer 收到的四条消息发送给 req
     * 1. routing_id: 是用来寻找路由的，并不是真实的发送出去
     * 2. 空帧
     * 3. GHI
     * 4. JKL
     */

    /**
     * req 收到了 router 发送过来的三条消息:
     * 1. 空帧: 该帧不会被递交给用户
     * 2. GHI
     * 3. JKL
     */

    //  Receive the reply.
    recv_string_expect_success (req, "GHI", 0);
    TEST_ASSERT_SUCCESS_ERRNO (zmq_getsockopt (req, ZMQ_RCVMORE, &rcvmore, &sz));
    TEST_ASSERT_TRUE (rcvmore);
    recv_string_expect_success (req, "JKL", 0);
    TEST_ASSERT_SUCCESS_ERRNO (zmq_getsockopt (req, ZMQ_RCVMORE, &rcvmore, &sz));
    TEST_ASSERT_FALSE (rcvmore);

    //  Clean up.
    test_context_socket_close (req);
    test_context_socket_close (rep);
    test_context_socket_close (router);
    test_context_socket_close (dealer);
}

int main ()
{
    setup_test_environment (60000);

    UNITY_BEGIN ();
    RUN_TEST (test_roundtrip);
    return UNITY_END ();
}
