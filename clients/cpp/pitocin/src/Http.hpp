#ifndef PITOCIN_HTTP_H
#define PITOCIN_HTTP_H

#include <string>
#include <cstring>
#include <sstream>

#include <functional>
#include <uv.h>
#include <cstdint>
#include <unistd.h>
#include <http_parser.h>

#include <iostream>

namespace Pitocin
{
using namespace std;
class Http
{
    uv_loop_t *loop;
    string host;
    string ip;
    string port;
    string path;
    string query;
    uv_getaddrinfo_t getAddrInfoReq;
    uv_tcp_t tcp;
    uv_connect_t connReq;
    uv_stream_t *conn;
    uv_timer_t timer;
    string url;
    uint64_t timeout = 0;
    function<void(int code, string &&)> cb;
    bool finished;
    string method;
    char recvBuffer[1024];
    http_parser respParser;
    http_parser_settings parserSettings;
    int respCode = 0;
    stringstream respBody;

public:
    Http() = delete;
    Http(uv_loop_t *loop)
        : loop(loop)
    {
    }

    ~Http()
    {
    }

    void setUrl(string &&reqUrl)
    {
        url = reqUrl;
    }

    void setTimeout(uint64_t t)
    {
        timeout = t;
    }

    void setCallback(function<void(int code, string &&)> &&callback)
    {
        cb = callback;
    }

    void startGetRequest()
    {
        int r;
        finished = false;
        parseUrl();
        if (finished)
        {
            return;
        }
        if (timeout)
        {
            r = uv_timer_init(loop, &timer);
            if (r == 0)
            {
                timer.data = this;
                uv_timer_start(
                    &timer,
                    [](uv_timer_t *handle) {
                        Http *http = static_cast<Http *>(handle->data);
                        if (http->finished)
                        {
                            return;
                        }
                        http->error(0);
                    },
                    timeout,
                    0);
            }
        }
        method = "GET";
        connect();
    }

private:
    /**
     *  code:
     *     0    -> 请求超时
     *    -1    -> url解析失败
     *    -2    -> schema不为http
     *    -3    -> dns获取ip请求失败
     *    -4    -> dns获取ip请求返回错误状态
     *    -5    -> dns获取ip转化ip失败
     *    -6    -> 初始化tcp失败
     *    -7    -> 链接请求返回错误状态
     *    -10   -> 链接读取ssize小于0
     *    -11   -> 请求写失败
     *    -12   -> 请求写回调status异常
     * */
    void error(int code)
    {
        if (cb)
        {
            cb(code, "");
        }
        finish();
    }

    void finish()
    {
        uv_handle_t *timerHandle = (uv_handle_t *)&timer;
        if (uv_is_active(timerHandle))
        {
            uv_timer_stop(&timer);
        }
        uv_handle_t *tcpHandle = (uv_handle_t *)&tcp;
        if (uv_is_active(tcpHandle))
        {
            uv_close(tcpHandle, [](uv_handle_t *) {});
        }
        finished = true;
    }

    void parseUrl()
    {
        struct http_parser_url p;
        http_parser_url_init(&p);
        if (http_parser_parse_url(url.c_str(), url.length(), 0, &p))
        {
            error(-1);
            return;
        }
        string schema = url.substr(p.field_data[UF_SCHEMA].off, p.field_data[UF_SCHEMA].len);
        // 暂时只支持http
        if (schema.compare("http") != 0)
        {
            error(-2);
            return;
        }
        host = url.substr(p.field_data[UF_HOST].off, p.field_data[UF_HOST].len);
        port = url.substr(p.field_data[UF_PORT].off, p.field_data[UF_PORT].len);
        path = url.substr(p.field_data[UF_PATH].off, p.field_data[UF_PATH].len);
        query = url.substr(p.field_data[UF_QUERY].off, p.field_data[UF_QUERY].len);
        ip = host;

        if (port.length() == 0)
        {
            port = "80";
        }
        if (path.length() == 0)
        {
            path = "/";
        }
    }

    void connect()
    {
        struct sockaddr_in connAddr;
        int r = uv_ip4_addr(ip.c_str(), stoi(port), &connAddr);
        if (r == 0)
        {
            r = uv_tcp_init(loop, &tcp);
            if (r)
            {
                error(-6);
                return;
            }
            tcp.data = this;
            connReq.data = this;
            r = uv_tcp_connect(
                &connReq,
                &tcp,
                (const sockaddr *)(&connAddr),
                [](uv_connect_t *req, int status) {
                    Http *self = static_cast<Http *>(req->data);
                    if (status)
                    {
                        self->error(-7);
                        return;
                    }
                    self->conn = req->handle;
                    self->conn->data = self;
                    if (self->method.compare("GET") == 0)
                    {
                        self->startRecvResponse();
                        self->sendGetRequest();
                    }
                });
        }
        else // dns获取ip
        {
            struct addrinfo hints;
            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_protocol = IPPROTO_TCP;

            getAddrInfoReq.data = this;

            // 这个方法调用有点长，里面包含一个lambda逻辑
            r = uv_getaddrinfo(
                loop,
                &getAddrInfoReq,
                [](uv_getaddrinfo_t *req, int status, struct addrinfo *res) {
                    Http *self = static_cast<Http *>(req->data);
                    if (status)
                    {
                        self->error(-4);
                        uv_freeaddrinfo(res);
                        return;
                    }
                    struct sockaddr *addr = res->ai_addr;
                    char ip[17]{0};
                    int r = uv_ip4_name((struct sockaddr_in *)addr, ip, 16);
                    uv_freeaddrinfo(res);
                    if (r)
                    {
                        self->error(-5);
                        return;
                    }
                    self->ip = string(ip);
                    self->connect();
                },
                ip.c_str(),
                nullptr,
                nullptr);

            if (r)
            {
                error(-3);
                return;
            }
        }
    }

    void startRecvResponse()
    {
        respBody.seekg(0);
        respBody.seekp(0);
        http_parser_init(&respParser, HTTP_RESPONSE);
        respParser.data = this;
        http_parser_settings_init(&parserSettings);
        parserSettings.on_status = [](http_parser *parser, const char *at, size_t length) -> int {
            Http *self = static_cast<Http *>(parser->data);
            self->respCode = parser->status_code;
            return 0;
        };
        parserSettings.on_body = [](http_parser *parser, const char *at, size_t length) -> int {
            Http *self = static_cast<Http *>(parser->data);
            string body(at, length);
            self->respBody << body;
            return 0;
        };

        int r = uv_read_start(
            conn,
            [](uv_handle_t *handle, size_t suggested_size, uv_buf_t *buf) {
                Http *self = static_cast<Http *>(handle->data);
                buf->base = self->recvBuffer;
                buf->len = sizeof(self->recvBuffer);
            },
            [](uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
                Http *self = static_cast<Http *>(stream->data);
                // cout << "--------------------nread:" << nread << endl;
                if (nread < 0)
                {
                    if (nread == UV_EOF)
                    {
                        if (self->cb)
                        {
                            self->cb(self->respCode, self->respBody.str());
                        }
                        self->finish();
                        return;
                    }
                    else
                    {
                        self->error(-10);
                        return;
                    }
                }
                if (nread == 0)
                {
                    return;
                }
                size_t parsed = http_parser_execute(&self->respParser, &self->parserSettings, buf->base, buf->len);
                // cout << "parsed:" << parsed << " nread:" << nread << endl;
            });
    }

    void sendGetRequest()
    {
        uv_buf_t reqBuf;
        stringstream request;

        // line 1
        request << "GET"
                << " " << path;
        if (query.length() > 0)
        {
            request << "?" << query;
        }
        request << " "
                << "HTTP/1.1\r\n";

        // header: host
        request << "Host:"
                << " " << host << "\r\n";
        // header: connection
        request << "Connection:"
                << " "
                << "close"
                << "\r\n";

        request << "\r\n";

        string reqData = request.str();
        reqBuf.base = (char *)reqData.data();
        reqBuf.len = reqData.length();

        uv_write_t *w = new uv_write_t;
        w->data = this;
        int r = uv_write(w, conn, &reqBuf, 1, [](uv_write_t *req, int status) {
            Http *self = static_cast<Http *>(req->data);
            delete req;
            if (status)
            {
                self->error(-12);
            }
        });
        if (r)
        {
            error(-11);
        }
    }
};
} // namespace Pitocin
#endif