/**
 * @file spqueue.cpp
 * @author your name (you@domain.com)
 * @brief
 * @version 0.1
 * @date 2022-01-25
 *
 * @copyright Copyright (c) 2022
 *
 */

#include <zmq.h>
#include <queue>
#include <string>
#include <assert.h>

#include "testutil.hpp"
#include "testutil_unity.hpp"

#define MAX_WORKERS 100

#define LRU_READY "\001" // 消息：worker 准备就绪

int main(void)
{
    void *ctx = zmq_ctx_new();
    void *frontend = zmq_socket(ctx, ZMQ_ROUTER);
    void *backend = zmq_socket(ctx, ZMQ_ROUTER);
    zmq_bind(frontend, "tcp://*:5555"); // client 端点
    zmq_bind(backend, "tcp://*:5556");  // worker 端点

    std::queue<std::string> worker_queue; // 存放可用 worker 的队列

    uint32_t routing_id;

    while (1)
    {
        // clang-format off
        zmq_pollitem_t items[] =
        {
            {backend, 0, ZMQ_POLLIN, 0},
            {frontend, 0, ZMQ_POLLIN, 0}
        };
        // clang-format on

        if (worker_queue.size())
        {
            zmq_poll(items, 2, -1);
        }
        else
        {
            zmq_poll(items, 1, -1);
        }

        // 接收 client 的消息
        if (items[0].revents & ZMQ_POLLIN)
        {
            zmq_msg_t msg;
            zmq_msg_init(&msg);
            zmq_msg_recv(&msg, frontend, 0);

            // TODO: 转发给 worker

            zmq_msg_close (&msg);
        }

        // 处理 worker 的消息
        if (items[1].revents & ZMQ_POLLIN)
        {
            zmq_msg_t msg;
            zmq_msg_init(&msg);
            zmq_msg_recv(&msg, backend, 0);
            // routing_id = zmq_msg_routing_id(&msg);

            assert(worker_queue.size() < MAX_WORKERS);
            worker_queue.push(std::to_string(routing_id));      // 用 routing_id 标识 worker

            // TODO: 如果不是 ready 的消息，则将消息转发给 client

            zmq_msg_close (&msg);
        }
    }

    // TODO: 清理环境

    return 0;
}
