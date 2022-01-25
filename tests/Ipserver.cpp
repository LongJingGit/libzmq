/**
 * @file Ipserver.cpp
 * @author your name (you@domain.com)
 * @brief
 * @version 0.1
 * @date 2022-01-25
 *
 * @copyright Copyright (c) 2022
 *
 * 将REQ套接字连接至 tcp://*:5555
 * 和hwserver程序类似，除了以下两点：
 * - 直接输出请求内容
 * - 随机地降慢运行速度，或中止程序，模拟崩溃
 */

#include "test_common.h"

int main(void)
{
    void *context = zmq_init(1);
    void *server = zmq_socket(context, ZMQ_REP);
    zmq_bind(server, "tcp://*:5555");

    int cycles = 0;
    while (1)
    {
        char buffer[10];
        int num_bytes = zmq_recv(server, buffer, 10, 0);
        cycles++;

        //  循环几次后开始模拟各种故障
        if (cycles > 3)
        {
            std::cout << "I: 模拟程序崩溃" << std::endl;
            break;
        }

        std::cout << "I: 正常请求: " << buffer << std::endl;
        sleep(1); //  耗时的处理过程
        zmq_send(server, buffer, sizeof(buffer), 0);
    }

    zmq_close(server);
    zmq_term(context);
    return 0;
}
