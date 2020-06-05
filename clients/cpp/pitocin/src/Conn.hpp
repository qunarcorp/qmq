#ifndef PITOCIN_CONN_H
#define PITOCIN_CONN_H

#include <uv.h>
#include <unistd.h>
#include <string>
#include <functional>
#include <cstring>

#include <iostream>

#include "StackPool.hpp"

using namespace std;

namespace Pitocin
{
class Conn
{
    struct WriteCtx
    {
        uv_write_t handle;
        function<void()> callback;
        void *data;
    };

    uv_loop_t *loop;
    string host;
    int port;
    uv_tcp_t tcp;
    int errCode = 0;
    function<void(int)> onTerminate;
    function<void(char *, size_t)> onRead;
    function<void()> onConnect;
    char *readBuf = nullptr;
    size_t readBufSize = 0;
    size_t readBufNewSize = 1024;
    StackPool<WriteCtx> writePool;

public:
    Conn() = delete;
    Conn(uv_loop_t *loop)
        : loop(loop)
    {
        onTerminate = [](int code) {};
    }
    ~Conn()
    {
        cout << "~Conn()" << endl;
        close();
        free(readBuf);
    }
    void setAddr(string &&host, int port)
    {
        this->host = host;
        this->port = port;
    }
    void setTerminateCallback(decltype(onTerminate) callback)
    {
        onTerminate = callback;
    }

    void setReadCallback(decltype(onRead) callback)
    {
        onRead = callback;
    }

    void setConnectCallback(decltype(onConnect) callback)
    {
        onConnect = callback;
    }

    void setReadBufSize(size_t s)
    {
        if (s > readBufNewSize)
        {
            readBufNewSize = s;
        }
    }

    bool isConnected()
    {
        uv_handle_t *tcpHandle = (uv_handle_t *)&tcp;
        if (uv_is_active(tcpHandle))
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    void connect()
    {
        struct sockaddr_in connAddr;
        int r = uv_ip4_addr(host.c_str(), port, &connAddr);
        if (r == 0)
        {
            r = uv_tcp_init(loop, &tcp);
            if (r)
            {
                error(r);
                return;
            }
            tcp.data = this;
            uv_connect_t *connReq = new uv_connect_t;
            connReq->data = this;
            r = uv_tcp_connect(
                connReq,
                &tcp,
                (const sockaddr *)(&connAddr),
                [](uv_connect_t *req, int status) {
                    Conn *self = (Conn *)(req->data);
                    delete req;
                    if (status)
                    {
                        self->error(status);
                        return;
                    }
                    if (self->onConnect)
                        self->onConnect();
                    self->startRead();
                });
        }
        else // dns获取ip
        {
            struct addrinfo hints;
            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_protocol = IPPROTO_TCP;

            uv_getaddrinfo_t *getAddrInfoReq = new uv_getaddrinfo_t;
            getAddrInfoReq->data = this;

            // 这个方法调用有点长，里面包含一个lambda逻辑
            r = uv_getaddrinfo(
                loop,
                getAddrInfoReq,
                [](uv_getaddrinfo_t *req, int status, struct addrinfo *res) {
                    Conn *self = (Conn *)(req->data);
                    delete req;
                    if (status)
                    {
                        self->error(status);
                        uv_freeaddrinfo(res);
                        return;
                    }
                    struct sockaddr *addr = res->ai_addr;
                    char ip[17]{0};
                    int r = uv_ip4_name((struct sockaddr_in *)addr, ip, 16);
                    uv_freeaddrinfo(res);
                    if (r)
                    {
                        self->error(r);
                        return;
                    }
                    self->host = string(ip);
                    self->connect();
                },
                host.c_str(),
                nullptr,
                &hints);

            if (r)
            {
                error(r);
                return;
            }
        }
    }

    void close()
    {
        if (onTerminate != nullptr)
        {
            auto callback(onTerminate);
            onTerminate = nullptr;
            callback(errCode);
            uv_handle_t *tcpHandle = (uv_handle_t *)&tcp;
            if (uv_is_active(tcpHandle))
            {
                uv_close(tcpHandle, nullptr);
            }
        }
    }

    void write(uv_buf_t buf, function<void()> callback)
    {
        // cout << "ConnSend:";
        // for (size_t i = 0; i < buf.len; ++i)
        // {
        //     cout << +((uint8_t)buf.base[i]) << " ";
        // }
        // cout << endl;
        if (writePool.empty())
        {
            writePool.emplace();
        }
        WriteCtx *wCtx = writePool.pop();
        wCtx->callback = callback;
        wCtx->data = this;
        wCtx->handle.data = wCtx;
        int r = uv_write(
            &wCtx->handle, (uv_stream_t *)&tcp,
            &buf, 1,
            [](uv_write_t *req, int status) {
                WriteCtx *ctx = (WriteCtx *)req->data;
                Conn *self = (Conn *)ctx->data;
                ctx->callback();
                self->writePool.push(ctx);
                if (status)
                {
                    self->error(status);
                }
            });
        if (r)
        {
            error(r);
        }
    }

private:
    void startRead()
    {
        uv_stream_t *stream = (uv_stream_t *)&tcp;
        int r = uv_read_start(
            stream,
            [](uv_handle_t *handle, size_t suggested, uv_buf_t *buf) {
                Conn *self = (Conn *)handle->data;
                if (self->readBufNewSize > self->readBufSize)
                {
                    char *newReadBuf = (char *)realloc(self->readBuf, self->readBufNewSize);
                    if (newReadBuf)
                    {
                        self->readBuf = newReadBuf;
                        self->readBufSize = self->readBufNewSize;
                    }
                }
                buf->base = self->readBuf;
                buf->len = self->readBufSize;
            },
            [](uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
                Conn *self = (Conn *)stream->data;
                if (nread == UV_EOF)
                {
                    self->close();
                    return;
                }
                if (nread < 0)
                {
                    self->error(nread);
                    return;
                }
                if (nread == 0)
                {
                    return;
                }
                // cout << "ConnRecv:";
                // for (size_t i = 0; i < (size_t)nread; ++i)
                // {
                //     cout << +((uint8_t)buf->base[i]) << " ";
                // }
                // cout << endl;
                if (self->onRead)
                    self->onRead(buf->base, (size_t)nread);
            });
    }

    void error(int code)
    {
        errCode = code;
        close();
    }
};
} // namespace Pitocin

#endif
