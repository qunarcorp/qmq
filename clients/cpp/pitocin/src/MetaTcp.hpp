#ifndef PITOCIN_META_TCP_H
#define PITOCIN_META_TCP_H

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

class MetaTcp
{
    struct ReqCtx
    {
        uint32_t id;
        function<void(int, Header *, MetaResponse *)> callback;
        uv_timer_t timeout;
        MetaTcp *meta;
    };

    uv_loop_t *loop;
    Conn conn;
    StackPool<BigendStream> sendStreamPool;
    BigendStream recvStream;
    uint32_t requestIdAcc = 0;
    /**
     * ReqCtx的指针不map，就得在pool中，以确保不会有泄漏
     * */
    unordered_map<uint32_t, ReqCtx *> reqMap;
    /**
     * 用来存放请求上下文的容器，为的是不要每次请求都使用new,delete．
     * 其中timer超时时间要参考exiledTimer的超时时间，确保exiled触发时，所有的req指针都回到了stack中
     * */
    StackPool<ReqCtx> reqPool;
    ExiledChecker checker;

public:
    static const int CodeSuccess = 0;
    static const int CodeTimeout = 999;
    static const int CodeRespParseError = 1;

    MetaTcp() = delete;
    MetaTcp(uv_loop_t *loop)
        : loop(loop),
          conn(loop),
          checker(loop),
          recvStream(1024)
    {
    }
    ~MetaTcp()
    {
        for (auto it = reqMap.begin(); it != reqMap.end(); ++it)
        {
            ReqCtx *req = it->second;
            uv_timer_stop(&req->timeout);
            req->callback(CodeTimeout, nullptr, nullptr);
            reqPool.push(req);
        }
        reqMap.clear();
        cout << "MetaTcp_Destruct" << endl;
    }
    void setAddr(string &&host, int port)
    {
        conn.setAddr(forward<string>(host), port);
    }
    void setConnectCallback(function<void(MetaTcp *)> callback)
    {
        conn.setConnectCallback([this, callback]() {
            callback(this);
        });
    }
    void setTerminateCallback(function<void(MetaTcp *, int code)> callback)
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
        conn.connect();
    }

    void metaRequest(
        Header *header,
        MetaRequest *metaReq,
        function<void(int, Header *, MetaResponse *)> callback)
    {
        checker.doRequest();
        header->code = Header::CodeClientRegister;
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
        metaReq->encode(*sendStream);
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
        if (reqPool.empty())
        {
            reqPool.emplace();
        }
        ReqCtx *reqCtx = reqPool.pop();
        reqCtx->id = header->requestId;
        reqCtx->callback = callback;
        uv_timer_init(loop, &reqCtx->timeout);
        reqCtx->timeout.data = reqCtx;
        reqCtx->meta = this;
        uv_timer_start(
            &reqCtx->timeout, [](uv_timer_t *t) {
                ReqCtx *req = (ReqCtx *)t->data;
                if (req->meta->reqMap.erase(req->id) > 0)
                {
                    req->callback(CodeTimeout, nullptr, nullptr);
                    req->meta->reqPool.push(req);
                }
            },
            3000, 0);
        reqMap.emplace(header->requestId, reqCtx);
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
    }

    // 将连接流放，如果timeout时间内没有请求，就自动关闭连接
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
        size_t positMark = recvStream.posit;
        uint32_t respLen = 0;
        recvStream.readError = false;
        while (true)
        {
            recvStream >> respLen;
            BigendReader content = recvStream.slice(respLen);
            if (content.readError)
            {
                break;
            }
            positMark = recvStream.posit;
            // header
            Header header;
            if (!header.decode(content))
            {
                break;
            }
            // heartbeat 100
            if (header.code == Header::CodeHeartbeat)
            {
                // cout << "RecvHeartbeat" << endl
                //      << header.toString() << endl;
                continue;
            }
            // success 0
            if (header.code == Header::CodeSuccess)
            {
                auto got = reqMap.find(header.requestId);
                if (got == reqMap.end())
                {
                    continue;
                }
                ReqCtx *reqCtx = got->second;
                reqMap.erase(got);
                uv_timer_stop(&reqCtx->timeout);
                MetaResponse resp;
                if (resp.decode(content))
                {
                    reqCtx->callback(CodeSuccess, &header, &resp);
                }
                else
                {
                    reqCtx->callback(CodeRespParseError, &header, nullptr);
                }
                reqCtx->meta->reqPool.push(reqCtx);
                continue;
            }
        }
        recvStream.posit = positMark;
        if (recvStream.posit > 0)
        {
            recvStream.compact();
        }
        return recvStream.posit + 4 + respLen;
    }
};
} // namespace Pitocin

#endif
