/*
    Copyright (c) 2007-2020 Contributors as noted in the AUTHORS file

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
#include <iostream>

SETUP_TEARDOWN_TESTCONTEXT

void test(const char *address)
{
    //  Create a router
    void *router = test_context_socket(ZMQ_ROUTER);
    char my_endpoint[MAX_SOCKET_STRING];

    //  set router socket options
    // 将 H 设置到了 options.hello_msg
    TEST_ASSERT_SUCCESS_ERRNO(zmq_setsockopt(router, ZMQ_HELLO_MSG, "H", 1));

    //  bind router
    test_bind(router, address, my_endpoint, MAX_SOCKET_STRING);

    //  Create a dealer
    void *dealer = test_context_socket(ZMQ_DEALER);
    TEST_ASSERT_SUCCESS_ERRNO(zmq_connect(dealer, my_endpoint));

    // Receive the hello message
    // 并没有经过网络读取数据，而是直接读取 options.hello_msg[0]。这和 router 通过 zmq_setsocketopt 将 H 设置到 options 吻合
    // hello_msg_session_t::pull_msg()
    recv_string_expect_success(dealer, "H", 0);

    //  Clean up.
    test_context_socket_close(dealer);
    test_context_socket_close(router);
}

void test_tcp()
{
    test("tcp://127.0.0.1:*");
}

void test_inproc()
{
    test("inproc://hello-msg");
}

void test_inproc_late_bind()
{
    char address[] = "inproc://late-hello-msg";

    //  Create a server
    void *server = test_context_socket(ZMQ_SERVER);

    //  set server socket options
    TEST_ASSERT_SUCCESS_ERRNO(zmq_setsockopt(server, ZMQ_HELLO_MSG, "W", 1));

    //  Create a dealer
    void *client = test_context_socket(ZMQ_CLIENT);
    TEST_ASSERT_SUCCESS_ERRNO(zmq_setsockopt(client, ZMQ_HELLO_MSG, "H", 1));
    TEST_ASSERT_SUCCESS_ERRNO(zmq_connect(client, address));

    //  bind server after the dealer
    TEST_ASSERT_SUCCESS_ERRNO(zmq_bind(server, address));

    // Receive the welcome message from server
    recv_string_expect_success(client, "W", 0);

    // Receive the hello message from client
    recv_string_expect_success(server, "H", 0);

    //  Clean up.
    test_context_socket_close(client);
    test_context_socket_close(server);
}

// 数据恢复请求
struct dataRecoverReq
{
    uint32_t step_size = 100; // 核心分片进行数据恢复时，每一次恢复的数据量。默认值 100，如果超过 100，则核心会限制为 100
    uint32_t time_out = 20;   // 数据恢复时，协议转换等待每一包数据的超时时间，单位s。默认值20s
    uint32_t sequence_no = 1; // 从当前序号开始进行数据恢复（必填）
};

#include <memory>
#include <cstring>
int main()
{
    setup_test_environment();

    UNITY_BEGIN();
    RUN_TEST(test_tcp);
    // RUN_TEST(test_inproc);
    // RUN_TEST(test_inproc_late_bind);
    return UNITY_END();
}
