pitocin
=======

An qmq client written in cpp with libuv

Build
-----

    $ make -f src/makefile

Usage
-----

main.cpp

```cpp
#include <uv.h>
#include <iostream>
#include <string>

#include "Client.hpp"

using namespace std;
using namespace Pitocin;

const int pubCount = 100000;

struct TaskCtx
{
    Client *client;
    uv_loop_t *loop;
    uv_timer_t timer;
    int number = 0;
    uint64_t begin = 0;
    uint64_t end = 0;
    int okCount = 0;
    int errCount = 0;
    int concurrent = 0;
};

void pub(TaskCtx *ctx)
{
    int number = ctx->number;
    string subject = "PitocinGibbonTest";
    vector<pair<string, string>> kvList;
    kvList.push_back(make_pair("number", to_string(number)));
    ctx->client->publish(
        subject,
        kvList,
        [ctx, number](string &msgId, int code) {
            if (code == 0)
            {
                ++(ctx->okCount);
            }
            else
            {
                ++(ctx->errCount);
            }
            --(ctx->concurrent);
            if (number == pubCount)
            {
                uv_timer_stop(&ctx->timer);
                ctx->end = uv_now(ctx->loop);
                cout << "==================>> beigin:" << ctx->begin
                     << " end:" << ctx->end
                     << " span:" << ctx->end - ctx->begin
                     << " ok:" << ctx->okCount
                     << " err:" << ctx->errCount
                     << endl;
            }
        });
}

void startPub(uv_timer_t *handle)
{
    TaskCtx *ctx = (TaskCtx *)handle->data;
    uint64_t now = uv_now(ctx->loop);
    if (ctx->begin == 0)
    {
        ctx->begin = now;
    }
    ctx->end = now;

    while (ctx->concurrent <= 1000)
    {
        if (ctx->number <= pubCount)
        {
            ++(ctx->number);
        }
        pub(ctx);
        ++(ctx->concurrent);
    }
    cout << "==================>> beigin:" << ctx->begin
         << " end:" << ctx->end
         << " span:" << ctx->end - ctx->begin
         << " ok:" << ctx->okCount
         << " err:" << ctx->errCount
         << endl;
}

int main()
{
    uv_loop_t *loop = uv_default_loop();
    cout << uv_version_string() << endl;

    Client client(loop);
    client.setBaseInfo("pitocin", "office", "dev");
    client.setErrCallback([](int code, string &&info) {
        cout << "ERR"
             << " code:" << code
             << " info:" << info << endl;
    });
    client.setHttpUrl("http://127.0.0.1:8080/meta/address");
    client.setHttpReqInterval(10000);
    client.start();

    TaskCtx ctx;
    ctx.client = &client;
    ctx.number = 0;
    ctx.loop = loop;

    uv_timer_init(loop, &ctx.timer);
    ctx.timer.data = &ctx;
    uv_timer_start(&ctx.timer, startPub, 3000, 100);

    uv_run(loop, UV_RUN_DEFAULT);
    return 0;
}
```

    $ g++ -std=c++14 -I ./src -o main main.cpp -luv -lhttp_parser