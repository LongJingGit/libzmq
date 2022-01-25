/**
 * @file Ipclient.cpp
 * @author your name (you@domain.com)
 * @brief
 * @version 0.1
 * @date 2022-01-25
 *
 * @copyright Copyright (c) 2022
 *
 * 使用zmq_poll轮询来实现安全的请求-应答
 * 运行时可随机关闭或重启lpserver程序
 */

#include "test_common.h"

#define REQUEST_TIMEOUT 2500 // 毫秒, (> 1000!)
#define REQUEST_RETRIES 10   // 尝试次数
#define ZMQ_POLL_MSEC 1      // epoll 超时时间

#define SERVER_ENDPOINT "tcp://localhost:5555"

int main(void)
{
    void *context = zmq_ctx_new();
    assert(context);
    std::cout << "正在连接服务器... " << std::endl;

    void *client = zmq_socket(context, ZMQ_REQ);
    assert(client);

    zmq_connect(client, SERVER_ENDPOINT);

    int sequence = 0;
    int retries_left = REQUEST_RETRIES;

    while (retries_left)
    {
        // 发送一个请求，并开始接收消息
        char request[10];
        sprintf(request, "%d", ++sequence);
        zmq_send(client, request, sizeof(request), 0);

        int expect_reply = 1;
        while (expect_reply)
        {
            // 对套接字进行轮询，并设置超时时间
            zmq_pollitem_t items[] = {{client, 0, ZMQ_POLLIN, 0}};
            int rc = zmq_poll(items, 1, REQUEST_TIMEOUT * ZMQ_POLL_MSEC);
            if (rc == -1)
                break; //  中断

            // 如果接收到回复则进行处理
            if (items[0].revents & ZMQ_POLLIN)
            {
                // 收到服务器应答，必须和请求时的序号一致
                char buffer[255];
                zmq_recv(client, buffer, sizeof(buffer), 0);
                if (!buffer)
                    break; //  Interrupted
                if (atoi(buffer) == sequence)
                {
                    std::cout << "I: 服务器返回正常 " << buffer << std::endl;
                    retries_left = REQUEST_RETRIES;
                    expect_reply = 0;
                }
                else
                {
                    std::cout << "E: 服务器返回异常: " << buffer << std::endl;
                }
            }
            // poll 超时，并且由已经到达最大的重试次数 retries_left == 0
            else if (--retries_left == 0)
            {
                std::cout << "E: 服务器不可用，取消操作" << std::endl;
                break;
            }
            // poll 超时，尝试重新连接
            else
            {
                std::cout << "W: 服务器没有响应，正在重试..." << std::endl;

                //  关闭旧套接字，并建立新套接字
                zmq_close(client);
                std::cout << "I: 服务器重连中..." << std::endl;
                client = zmq_socket(context, ZMQ_REQ);
                zmq_connect(client, SERVER_ENDPOINT);

                //  使用新套接字再次发送请求
                zmq_send(client, request, sizeof(request), 0);
            }
        }
    }

    zmq_close(client);
    zmq_term(context);
    return 0;
}
