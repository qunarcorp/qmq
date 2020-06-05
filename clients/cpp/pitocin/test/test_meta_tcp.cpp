#include <uv.h>
#include <iostream>
// #include <memory>
#include <functional>

#include "MetaTcp.hpp"

int main()
{
    uv_loop_t *loop = uv_default_loop();
    auto tcp = new Pitocin::MetaTcp(loop);
    tcp->setAddr("10.86.37.226", 20881);
    tcp->setOnConnectCallback([](Pitocin::MetaTcp *mt) {
        std::cout << "MetaTcp Connected" << std::endl;

        mt->heartbeat();

        Pitocin::Header reqHeader;

        Pitocin::MetaRequest req;
        req.subject = "PitocinTest";
        req.clientTypeCode = '1';
        req.appCode = "pitocin";
        req.requestType = '1';
        req.clientId = "192.168.50.12@pitocin";
        req.room = "office";
        req.env = "dev";

        auto callback = [mt](int code, Pitocin::Header *respHeader, Pitocin::MetaResponse *resp) {
            std::cout << "RespCode:" << code << std::endl;
            if (code == Pitocin::MetaTcp::CodeSuccess)
            {
                std::cout << respHeader->toString() << std::endl
                          << resp->toString() << std::endl;
            }
        };
        mt->metaRequest(&reqHeader, &req, callback);

    });
    tcp->setOnDisconnectCallback([](Pitocin::MetaTcp *mt) {
        std::cout << "MetaTcp Disconnected" << std::endl;
        delete mt;
    });
    tcp->setExile(3000);
    tcp->connect();
    uv_run(loop, UV_RUN_DEFAULT);
    return 0;
}
