#ifndef PITOCIN_BROKER_TCP_H
#define PITOCIN_BROKER_TCP_H

#include <uv.h>
#include "Conn.hpp"
#include "Protocol.hpp"
#include "StackPool.hpp"
#include "ExiledChecker.hpp"

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>

#include <iostream>

using namespace std;

namespace Pitocin
{
class BrokerTcp
{
    struct PublishCtx
    {
        uint32_t id;
        function<void(int, Header *, PublishMessageResult *)> callback;
        uv_timer_t timeout;
        BrokerTcp *broker;
    };

    uv_loop_t *loop;
    Conn conn;
    BrokerInfo info;

    StackPool<BigendStream> sendStreamPool;
    BigendStream recvStream;
    uint32_t requestIdAcc = 0;
    unordered_map<uint32_t, PublishCtx *> publishMap;
    StackPool<PublishCtx> publishPool;

    ExiledChecker checker;

public:
    static const int CodeSuccess = 0;
    static const int CodeTimeout = 999;
    static const int CodePaseError = 1;

    BrokerTcp() = delete;
    BrokerTcp(uv_loop_t *loop)
        : loop(loop),
          conn(loop),
          checker(loop),
          recvStream(1024)
    {
    }
    ~BrokerTcp()
    {
        cout << "BrokerTcp_Destruct" << endl;
    }
    void setInfo(BrokerInfo &brokerInfo)
    {
        info = brokerInfo;
        conn.setAddr(move(info.ip), info.port);
    }
    BrokerInfo &getInfo()
    {
        return info;
    }
    void setConnectCallback(function<void(BrokerTcp *)> callback)
    {
        conn.setConnectCallback([this, callback]() {
            callback(this);
        });
    }
    void setTerminateCallback(function<void(BrokerTcp *, int code)> callback)
    {
        conn.setTerminateCallback([this, callback](int code) {
            callback(this, code);
        });
    }
    void connect()
    {
        conn.setReadCallback([this](char *base, size_t len) {
            this->recvStream.write((uint8_t *)base, len);
            size_t suggestBufLen = this->dealReadData();
            this->conn.setReadBufSize(suggestBufLen);
        });
        cout << "conn.connect()" << endl;
        conn.connect();
    }

    void publish(
        Header *header,
        PublishMessage *msg,
        function<void(int, Header *, PublishMessageResult *)> callback)
    {
        checker.doRequest();
        header->code = Header::CodeSendMessage;
        header->requestId = ++requestIdAcc;
        header->requestCode = header->code;
        header->flag = Header::FlagRequest;

        if (sendStreamPool.empty())
        {
            sendStreamPool.emplace();
        }
        BigendStream *sendStream = sendStreamPool.pop();
        sendStream->reset();
        *sendStream << (uint32_t)0;
        header->encode(*sendStream);
        msg->encode(*sendStream);
        uint32_t len = sendStream->size - 4;
        sendStream->insert(len, 0);
        uv_buf_t uvBuf{
            .base = (char *)sendStream->buf,
            .len = sendStream->size};
        conn.write(
            uvBuf,
            [this, sendStream]() {
                this->sendStreamPool.push(sendStream);
            });

        // 下面的代码是处理超时的
        if (publishPool.empty())
        {
            publishPool.emplace();
        }
        PublishCtx *pubCtx = publishPool.pop();
        pubCtx->id = header->requestId;
        pubCtx->callback = callback;
        uv_timer_init(loop, &pubCtx->timeout);
        pubCtx->timeout.data = pubCtx;
        pubCtx->broker = this;
        uv_timer_start(
            &pubCtx->timeout,
            [](uv_timer_t *t) {
                PublishCtx *pub = (PublishCtx *)t->data;
                if (pub->broker->publishMap.erase(pub->id) > 0)
                {
                    pub->callback(CodeTimeout, nullptr, nullptr);
                    pub->broker->publishPool.push(pub);
                }
            },
            10000, 0);
        publishMap.emplace(header->requestId, pubCtx);
    }

    void heartbeat()
    {
        checker.doRequest();
        Header header;
        header.code = Header::CodeHeartbeat;
        header.requestId = ++requestIdAcc;
        header.requestCode = header.code;
        if (sendStreamPool.empty())
        {
            sendStreamPool.emplace();
        }
        BigendStream *sendStream = sendStreamPool.pop();
        sendStream->reset();
        *sendStream << (uint32_t)0;
        header.encode(*sendStream);
        uint32_t len = sendStream->size - 4;
        sendStream->insert(len, 0);
        uv_buf_t uvBuf{
            .base = (char *)sendStream->buf,
            .len = sendStream->size};
        conn.write(
            uvBuf,
            [this, sendStream]() {
                this->sendStreamPool.push(sendStream);
            });
        cout << "broker-heartbeat" << endl;
    }

    void setExile(uint64_t timeout)
    {
        checker.setInterval(timeout);
        checker.setCallback([this](uint64_t timestamp) {
            this->conn.close();
        });
        checker.start();
    }

private:
    size_t dealReadData()
    {
        // cout << recvStream.toString() << endl;
        size_t positMark = recvStream.posit;
        uint32_t binLen = 0;
        recvStream.readError = false;
        while (true)
        {
            recvStream >> binLen;
            BigendReader content = recvStream.slice(binLen);
            if (recvStream.readError)
            {
                break;
            }
            positMark = recvStream.posit;
            Header header;
            if (!header.decode(content))
            {
                break;
            }
            // heartbeat 100
            if (header.code == Header::CodeHeartbeat)
            {
                cout << "RecvHeartbeat" << endl
                     << header.toString() << endl;
                continue;
            }
            // publish
            if (header.requestCode == Header::CodeSendMessage)
            {
                auto got = publishMap.find(header.requestId);
                if (got == publishMap.end())
                {
                    continue;
                }
                PublishCtx *pubCtx = got->second;
                publishMap.erase(got);
                uv_timer_stop(&pubCtx->timeout);
                PublishMessageResult resp;
                if (resp.decode(content))
                {
                    pubCtx->callback(CodeSuccess, &header, &resp);
                }
                else
                {
                    pubCtx->callback(CodePaseError, &header, nullptr);
                }
                pubCtx->broker->publishPool.push(pubCtx);
                continue;
            }
        }
        recvStream.posit = positMark;
        if (recvStream.posit > 0)
        {
            recvStream.compact();
        }
        return recvStream.posit + 4 + binLen;
    }
};
} // namespace Pitocin

#endif //PITOCIN_BROKER_TCP_H
