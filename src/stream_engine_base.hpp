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

#ifndef __ZMQ_STREAM_ENGINE_BASE_HPP_INCLUDED__
#define __ZMQ_STREAM_ENGINE_BASE_HPP_INCLUDED__

#include <stddef.h>

#include "fd.hpp"
#include "i_engine.hpp"
#include "io_object.hpp"
#include "i_encoder.hpp"
#include "i_decoder.hpp"
#include "options.hpp"
#include "socket_base.hpp"
#include "metadata.hpp"
#include "msg.hpp"
#include "tcp.hpp"

namespace zmq {
class io_thread_t;
class session_base_t;
class mechanism_t;

//  This engine handles any socket with SOCK_STREAM semantics,
//  e.g. TCP socket or an UNIX domain socket.

class stream_engine_base_t : public io_object_t, public i_engine
{
public:
    stream_engine_base_t(fd_t fd_, const options_t &options_, const endpoint_uri_pair_t &endpoint_uri_pair_, bool has_handshake_stage_);
    ~stream_engine_base_t() ZMQ_OVERRIDE;

    //  i_engine interface implementation.
    bool has_handshake_stage() ZMQ_FINAL
    {
        return _has_handshake_stage;
    };
    void plug(zmq::io_thread_t *io_thread_, zmq::session_base_t *session_) ZMQ_FINAL;
    void terminate() ZMQ_FINAL;
    bool restart_input() ZMQ_FINAL;
    void restart_output() ZMQ_FINAL;
    void zap_msg_available() ZMQ_FINAL;
    const endpoint_uri_pair_t &get_endpoint() const ZMQ_FINAL;

    //  i_poll_events interface implementation.
    void in_event() ZMQ_FINAL;
    void out_event();
    void timer_event(int id_) ZMQ_FINAL;

protected:
    typedef metadata_t::dict_t properties_t;
    bool init_properties(properties_t &properties_);

    //  Function to handle network disconnections.
    virtual void error(error_reason_t reason_);

    int next_handshake_command(msg_t *msg_);
    int process_handshake_command(msg_t *msg_);

    int pull_msg_from_session(msg_t *msg_);
    int push_msg_to_session(msg_t *msg_);

    int pull_and_encode(msg_t *msg_);
    virtual int decode_and_push(msg_t *msg_);
    int push_one_then_decode_and_push(msg_t *msg_);

    void set_handshake_timer();

    virtual bool handshake()
    {
        return true;
    };
    virtual void plug_internal(){};

    virtual int process_command_message(msg_t *msg_)
    {
        LIBZMQ_UNUSED(msg_);
        return -1;
    };
    virtual int produce_ping_message(msg_t *msg_)
    {
        LIBZMQ_UNUSED(msg_);
        return -1;
    };
    virtual int process_heartbeat_message(msg_t *msg_)
    {
        LIBZMQ_UNUSED(msg_);
        return -1;
    };
    virtual int produce_pong_message(msg_t *msg_)
    {
        LIBZMQ_UNUSED(msg_);
        return -1;
    };

    virtual int read(void *data, size_t size_);
    virtual int write(const void *data_, size_t size_);

    void reset_pollout()
    {
        io_object_t::reset_pollout(_handle);
    }
    void set_pollout()
    {
        io_object_t::set_pollout(_handle);
    }
    void set_pollin()
    {
        io_object_t::set_pollin(_handle);
    }
    session_base_t *session()
    {
        return _session;
    }
    socket_base_t *socket()
    {
        return _socket;
    }

    const options_t _options;

    unsigned char *_inpos;
    size_t _insize;
    i_decoder *_decoder;

    unsigned char *_outpos;
    size_t _outsize;
    i_encoder *_encoder;

    mechanism_t *_mechanism;            // ZMTP 协议使用

    /**
     * 读写不同类型的消息时，这两个函数指针的指向会不断变化（以下以 zmtp_engine_t 为例）：
     *
     * 1. 在 zmtp_engine_t 的构造函数中，函数指针分别指向 routing_id_msg 和 process_routing_id_msg
     *
     * 2.1 在 stream_engine_base_t::out_event() 中，双方分别给对端发送 greeting 消息(该消息已经提前在 zmtp_engine_t::plug_internal 中构造完成，该消息用于双方协商版本号和安全机制);
     *
     * 2.2 在 stream_engine_base_t::in_event() --> handshake() 中分别接收对端 greeting 消息，双方协商版本号，如果是 zmtp_v3.0 和 v3.1 版本，则会分别指向 next_handshake_command 和 process_handshake_command, handshake() 返回 true;
     *
     * 2.3 在 next_handshake_command 中, 双方构造一条 routing_id 消息发送给对方:
     *     2.3.1 req/dealer/router 类型的 socket 构造的这条消息的 name 为 ZMTP_PROPERTY_IDENTITY, 对端收到之后会创建 UUID 标识
     *     2.3.2 其他类型的 socket 构造的这条消息 name 为默认值，所以对端收到之后并不会创建 UUID
     *
     * 2.4 在 process_handshake_command 中, 做了如下几件事：
     *     2.4.1 将从内核接收到的 routing_id 消息进行解析: 如果 name 为 ZMTP_PROPERTY_IDENTITY, 则会创建 UUID; 否则不会创建
     *     2.4.2 创建 session 和 socket 通信的 pipe, 发送命令通知 socket bind 该 pipe, 然后将 routing_id 消息推送给 socket, 由 socket 为对端生成 UUID
     *     2.4.3 将 _next_msg 和 _process_msg 的指向更改为 pull_and_encode 和 write_credential，开始处理正常的用户数据
     *
     * 2.5 pull_and_encode 用于从 session pull 数据，然后发送给内核;
     *
     * 2.6 在 write_credential 中，_process_msg 又被指向了 decode_and_push, 该接口用于将从内核读取出来的数据 push 给 session. 需要注意的是：_process_msg 函数指针的指向在后续的处理过程中会根据实际情况，不断变更该函数指针的指向.
     *
     */
    int (stream_engine_base_t::*_next_msg)(msg_t *msg_);
    int (stream_engine_base_t::*_process_msg)(msg_t *msg_);

    //  Metadata to be attached to received messages. May be NULL.
    metadata_t *_metadata;

    //  True iff the engine couldn't consume the last decoded message.
    bool _input_stopped;

    //  True iff the engine doesn't have any message to encode.
    bool _output_stopped;

    //  Representation of the connected endpoints.
    const endpoint_uri_pair_t _endpoint_uri_pair;

    //  ID of the handshake timer
    enum
    {
        handshake_timer_id = 0x40
    };

    //  True is linger timer is running.
    bool _has_handshake_timer;

    //  Heartbeat stuff
    enum
    {
        heartbeat_ivl_timer_id = 0x80,
        heartbeat_timeout_timer_id = 0x81,
        heartbeat_ttl_timer_id = 0x82
    };
    bool _has_ttl_timer;
    bool _has_timeout_timer;
    bool _has_heartbeat_timer;

    const std::string _peer_address;

private:
    bool in_event_internal();

    //  Unplug the engine from the session.
    void unplug();

    int write_credential(msg_t *msg_);

    void mechanism_ready();

    //  Underlying socket.
    fd_t _s;            // conn_fd，engine 通过该 socket 与内核交换数据（创建 engine 的时候初始化的）

    handle_t _handle;

    bool _plugged;

    //  When true, we are still trying to determine whether
    //  the peer is using versioned protocol, and if so, which
    //  version.  When false, normal message flow has started.
    bool _handshaking;  // 在构造函数中被初始化成了 true

    msg_t _tx_msg;

    bool _io_error;

    //  The session this engine is attached to.
    zmq::session_base_t *_session; // stream_engine_base_t::plug 时将创建的 session 和 engine 绑定到了一起

    //  Socket
    zmq::socket_base_t *_socket;        // 本端 socket

    //  Indicate if engine has an handshake stage, if it does, engine must call session.engine_ready
    //  when handshake is completed.
    // zmtp_engine: true        raw_engine: false
    bool _has_handshake_stage;

    ZMQ_NON_COPYABLE_NOR_MOVABLE(stream_engine_base_t)
};
} // namespace zmq

#endif
