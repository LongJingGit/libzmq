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
 * @brief 请求--应答     1 --> N 模式           N --> 1 模式
 *
 * 请求应答模式的特点：REQ 发起请求，必须等到 REP 的响应之后，REQ 才可以发送第二条请求
 */
char connect_address[MAX_SOCKET_STRING];

void test_round_robin_out (const char *bind_address_)
{
    void *req = test_context_socket (ZMQ_REQ);

    TEST_ASSERT_SUCCESS_ERRNO (zmq_bind (req, bind_address_));
    size_t len = MAX_SOCKET_STRING;
    TEST_ASSERT_SUCCESS_ERRNO (zmq_getsockopt (req, ZMQ_LAST_ENDPOINT, connect_address, &len));

    const size_t services = 1;
    void *rep[services];
    for (size_t peer = 0; peer < services; peer++)
    {
        rep[peer] = test_context_socket (ZMQ_REP);

        TEST_ASSERT_SUCCESS_ERRNO (zmq_connect (rep[peer], connect_address));
    }

    /**
     * @brief 信封的生成过程：
     * 1. connect 成功之后，触发 req 的 EPOLLOUT 事件，req 会在 out_event() 中构造一条 routing_id 的消息发送给 rep
     * 2. rep 触发 EPOLLIN 事件，在 in_event() 中接收该 routing_id 的消息并递交给 rep socket，然后给 rep socket 发送 activate_read 命令读取该消息
     * 3. 当 rep 在调用 recv 接收真实的用户消息之前，会先处理 pending 的命令。比如处理 activate_read 命令：读取 routing_id 消息，然后在 xread_activated 中接收该消息并为对端生成 UUID 标识
     *
     * 具体的代码可以参考 stream_engine_base_t::out_event() 和 stream_engine_base_t::in_event()
     */

    //  We have to give the connects time to finish otherwise the requests
    //  will not properly round-robin. We could alternatively connect the
    //  REQ sockets to the REP sockets.
    msleep (SETTLE_TIME);

    // s_send_seq (req, "ABC", SEQ_END);
    // s_recv_seq (rep[0], "ABC", SEQ_END);
    // s_send_seq (req, "ABC", SEQ_END);            // REQ 没有收到 REP 的响应，所以无法发送第二条请求

    // REQ 发送多帧消息，则需要使用 ZMQ_SNDMORE（参考 test_reqrep_device.cpp）
    // send_string_expect_success (req, "ABC", ZMQ_SNDMORE);
    // send_string_expect_success (req, "DEF", 0);

    // recv_string_expect_success (rep[0], "ABC", 0);
    // recv_string_expect_success (rep[0], "DEF", 0);

    /**
     * req 和 rep 连接刚建立起来的时候，req 就会发送一条空消息给 rep(rep 继承于 router)，该空消息用于生成 UUID。
     *
     * rep 收到该空消息之后，由 engine 读取该消息，并由 session 将消息写入到和 socket 通信的管道，然后发送命令给 socket，
     *
     * rep 在接收消息前，会先处理 pending 的命令。该 pending 命令就是 session 发送过来的命令，然后激活 rep 的 read,
     *
     * rep 读取空消息，并为该管道生成特定的 UUID，用于标识对端的 socket
     */
    // s_recv_seq (rep[0], "ABC", SEQ_END);     // 会在 router_t::xread_activated 中接收发送过来的空消息，然后生成 UUID

    /**
     * req 发送请求的时候，会先发送一条长度为 0 的空消息给对端，该空消息为了请求 routing_id；然后再发送正常的用户消息
     * rep 接收到消息也是先接收到空消息(直接转发给对端)，然后接收正常的用户消息
     */

    // Send our peer-replies, and expect every REP it used once in order
    for (size_t peer = 0; peer < services; peer++)
    {
        s_send_seq (req, "ABC", SEQ_END);
        s_recv_seq (rep[peer], "ABC", SEQ_END);
        s_send_seq (rep[peer], "DEF", SEQ_END);          // 给请求端 REQ 发送响应，如果不发送响应，则请求端无法发送第二条请求
        s_recv_seq (req, "DEF", SEQ_END);       // req 接收到响应之后才可以发送第二条请求
    }

    test_context_socket_close_zero_linger (req);
    for (size_t peer = 0; peer < services; peer++)
    {
        test_context_socket_close_zero_linger (rep[peer]);
    }
}

void test_req_only_listens_to_current_peer (const char *bind_address_)
{
    void *req = test_context_socket (ZMQ_REQ);

    TEST_ASSERT_SUCCESS_ERRNO (zmq_setsockopt (req, ZMQ_ROUTING_ID, "A", 2)); // 设置 REQ 的（套接字标识）

    TEST_ASSERT_SUCCESS_ERRNO (zmq_bind (req, bind_address_));
    size_t len = MAX_SOCKET_STRING;
    TEST_ASSERT_SUCCESS_ERRNO (zmq_getsockopt (req, ZMQ_LAST_ENDPOINT, connect_address, &len));

    const size_t services = 3;
    void *router[services];

    for (size_t i = 0; i < services; ++i)
    {
        router[i] = test_context_socket (ZMQ_ROUTER);

        int enabled = 1;
        TEST_ASSERT_SUCCESS_ERRNO (zmq_setsockopt (router[i], ZMQ_ROUTER_MANDATORY, &enabled, sizeof (enabled)));

        TEST_ASSERT_SUCCESS_ERRNO (zmq_connect (router[i], connect_address));
    }

    // Wait for connects to finish.
    msleep (SETTLE_TIME);

    for (size_t i = 0; i < services; ++i)
    {
        // There still is a race condition when a stale peer's message
        // arrives at the REQ just after a request was sent to that peer.
        // To avoid that happening in the test, sleep for a bit.
        TEST_ASSERT_EQUAL_INT (1, TEST_ASSERT_SUCCESS_ERRNO (zmq_poll (0, 0, 10)));

        s_send_seq (req, "ABC", SEQ_END);
        // zmq_send(req, "ABC", strlen("ABC"), SEQ_END);

        // Receive on router i
        s_recv_seq (router[i], "A", 0, "ABC", SEQ_END);
        // recv_string_expect_success (router[i], "A", 0);
        // recv_string_expect_success (router[i], "ABC", 0);

        // Send back replies on all routers
        for (size_t j = 0; j < services; ++j)
        {
            const char *replies[] = {"WRONG", "GOOD"};
            const char *reply = replies[i == j ? 1 : 0];
            s_send_seq (router[j], "A", 0, reply, SEQ_END);
        }

        // Receive only the good reply
        s_recv_seq (req, "GOOD", SEQ_END);
    }

    test_context_socket_close_zero_linger (req);
    for (size_t i = 0; i < services; ++i)
        test_context_socket_close_zero_linger (router[i]);
}

void test_req_message_format (const char *bind_address_)
{
    void *req = test_context_socket (ZMQ_REQ);
    void *router = test_context_socket (ZMQ_ROUTER);

    TEST_ASSERT_SUCCESS_ERRNO (zmq_bind (req, bind_address_));
    size_t len = MAX_SOCKET_STRING;
    TEST_ASSERT_SUCCESS_ERRNO (
      zmq_getsockopt (req, ZMQ_LAST_ENDPOINT, connect_address, &len));

    TEST_ASSERT_SUCCESS_ERRNO (zmq_connect (router, connect_address));

    // Send a multi-part request.
    s_send_seq (req, "ABC", "DEF", SEQ_END);

    zmq_msg_t msg;
    zmq_msg_init (&msg);

    // Receive peer routing id
    TEST_ASSERT_SUCCESS_ERRNO (zmq_msg_recv (&msg, router, 0));
    TEST_ASSERT_GREATER_THAN_INT (0, zmq_msg_size (&msg));
    zmq_msg_t peer_id_msg;
    zmq_msg_init (&peer_id_msg);
    zmq_msg_copy (&peer_id_msg, &msg);

    int more = 0;
    size_t more_size = sizeof (more);
    TEST_ASSERT_SUCCESS_ERRNO (
      zmq_getsockopt (router, ZMQ_RCVMORE, &more, &more_size));
    TEST_ASSERT_TRUE (more);

    // Receive the rest.
    s_recv_seq (router, 0, "ABC", "DEF", SEQ_END);

    // Send back a single-part reply.
    TEST_ASSERT_SUCCESS_ERRNO (
      zmq_msg_send (&peer_id_msg, router, ZMQ_SNDMORE));
    s_send_seq (router, 0, "GHI", SEQ_END);

    // Receive reply.
    s_recv_seq (req, "GHI", SEQ_END);

    TEST_ASSERT_SUCCESS_ERRNO (zmq_msg_close (&msg));
    TEST_ASSERT_SUCCESS_ERRNO (zmq_msg_close (&peer_id_msg));

    test_context_socket_close_zero_linger (req);
    test_context_socket_close_zero_linger (router);
}

void test_block_on_send_no_peers ()
{
    void *sc = test_context_socket (ZMQ_REQ);

    int timeout = 250;
    TEST_ASSERT_SUCCESS_ERRNO (
      zmq_setsockopt (sc, ZMQ_SNDTIMEO, &timeout, sizeof (timeout)));

    TEST_ASSERT_FAILURE_ERRNO (EAGAIN, zmq_send (sc, 0, 0, ZMQ_DONTWAIT));
    TEST_ASSERT_FAILURE_ERRNO (EAGAIN, zmq_send (sc, 0, 0, 0));

    test_context_socket_close (sc);
}

const char bind_inproc[] = "inproc://a";
const char bind_tcp[] = "tcp://127.0.0.1:*";

void test_round_robin_out_inproc ()
{
    test_round_robin_out (bind_inproc);
}

void test_round_robin_out_tcp ()
{
    test_round_robin_out (bind_tcp);
}

void test_req_message_format_inproc ()
{
    test_req_message_format (bind_inproc);
}

void test_req_message_format_tcp ()
{
    test_req_message_format (bind_tcp);
}

void test_req_only_listens_to_current_peer_inproc ()
{
    test_req_only_listens_to_current_peer (bind_inproc);
}

void test_req_only_listens_to_current_peer_tcp ()
{
    test_req_only_listens_to_current_peer (bind_tcp);
}

int main ()
{
    setup_test_environment (6000);

    UNITY_BEGIN ();

    // SHALL route outgoing messages to connected peers using a round-robin
    // strategy.
    // RUN_TEST (test_round_robin_out_inproc);
    RUN_TEST (test_round_robin_out_tcp);

    // The request and reply messages SHALL have this format on the wire:
    // * A delimiter, consisting of an empty frame, added by the REQ socket.
    // * One or more data frames, comprising the message visible to the
    //   application.
    // RUN_TEST (test_req_message_format_inproc);
    // RUN_TEST (test_req_message_format_tcp);

    // SHALL block on sending, or return a suitable error, when it has no
    // connected peers.
    // RUN_TEST (test_block_on_send_no_peers);

    // SHALL accept an incoming message only from the last peer that it sent a
    // request to.
    // SHALL discard silently any messages received from other peers.
    // TODO PH: this test is still failing; disabled for now to allow build to
    // complete.
    // RUN_TEST (test_req_only_listens_to_current_peer_inproc);
    // RUN_TEST (test_req_only_listens_to_current_peer_tcp);

    return UNITY_END ();
}
