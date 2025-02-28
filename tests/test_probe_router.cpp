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

#include <iostream>
void test_probe_router_router ()
{
    //  Create server and bind to endpoint
    void *server = test_context_socket (ZMQ_ROUTER);

    char my_endpoint[MAX_SOCKET_STRING];
    bind_loopback_ipv4 (server, my_endpoint, sizeof (my_endpoint));

    //  Create client and connect to server, doing a probe
    void *client = test_context_socket (ZMQ_ROUTER);
    TEST_ASSERT_SUCCESS_ERRNO (zmq_setsockopt (client, ZMQ_ROUTING_ID, "X", 1));    // 用 ZMQ_ROUTING_ID 设置套接字标识
    int probe = 1;
    TEST_ASSERT_SUCCESS_ERRNO (zmq_setsockopt (client, ZMQ_PROBE_ROUTER, &probe, sizeof (probe)));  // 给 socket 设置属性
    // 给对端发送空消息，消息来源为 "X"（在 zmq_connect --> xattach_pipe 的时候发送）
    TEST_ASSERT_SUCCESS_ERRNO (zmq_connect (client, my_endpoint));

    //  We expect a routing id=X + empty message from client
    recv_string_expect_success (server, "X", 0);    // 收到消息来源为 "X" 的空消息
    unsigned char buffer[255];
    TEST_ASSERT_EQUAL_INT (0, TEST_ASSERT_SUCCESS_ERRNO (zmq_recv (server, buffer, 255, 0)));

    //  Send a message to client now
    send_string_expect_success (server, "X", ZMQ_SNDMORE); // 寻找套接字表示为 "X" 的路由（实际上该消息不会被发送，只用来寻找路由）
    send_string_expect_success (server, "Hello", 0);      // 向上一步寻找到的 "X" 的套接字发送消息

    // receive the routing ID, which is auto-generated in this case, since the
    // peer did not set one explicitly
    /**
     * @brief 接收长度为 5 的空消息(routing_id 是本端自动生成的)
     *
     * 问题：
     * 1. 为什么该空消息长度为 5？
     * 2. 该空消息是 server 端什么时候发送的？
     * 3. 该空消息的接收为什么不能在 server 端 send 之前？
     */
    TEST_ASSERT_EQUAL_INT (5, TEST_ASSERT_SUCCESS_ERRNO (zmq_recv (client, buffer, 255, 0)));

    recv_string_expect_success (client, "Hello", 0);

    test_context_socket_close (server);
    test_context_socket_close (client);
}

void test_probe_router_dealer ()
{
    //  Create server and bind to endpoint
    void *server = test_context_socket (ZMQ_ROUTER);

    char my_endpoint[MAX_SOCKET_STRING];
    bind_loopback_ipv4 (server, my_endpoint, sizeof (my_endpoint));

    //  Create client and connect to server, doing a probe
    void *client = test_context_socket (ZMQ_DEALER);
    TEST_ASSERT_SUCCESS_ERRNO (zmq_setsockopt (client, ZMQ_ROUTING_ID, "X", 1));
    int probe = 1;
    TEST_ASSERT_SUCCESS_ERRNO (
      zmq_setsockopt (client, ZMQ_PROBE_ROUTER, &probe, sizeof (probe)));
    TEST_ASSERT_SUCCESS_ERRNO (zmq_connect (client, my_endpoint));

    //  We expect a routing id=X + empty message from client
    recv_string_expect_success (server, "X", 0);
    unsigned char buffer[255];
    TEST_ASSERT_SUCCESS_ERRNO (zmq_recv (server, buffer, 255, 0));

    //  Send a message to client now
    send_string_expect_success (server, "X", ZMQ_SNDMORE);
    send_string_expect_success (server, "Hello", 0);

    recv_string_expect_success (client, "Hello", 0);

    test_context_socket_close (server);
    test_context_socket_close (client);
}

int main ()
{
    setup_test_environment (6000);

    UNITY_BEGIN ();

    RUN_TEST (test_probe_router_router);
    RUN_TEST (test_probe_router_dealer);

    return UNITY_END ();
}
