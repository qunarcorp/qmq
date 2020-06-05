#include <uv.h>
#include <iostream>
#include <functional>
#include "BrokerTcp.hpp"
#include "Protocol.hpp"
#include "RequestValve.hpp"

using namespace std;
using namespace Pitocin;

int main()
{
    uv_loop_t *loop = uv_default_loop();

    RequestValve<BrokerTcp> valve(loop);

    auto task = [loop](RequestValve<BrokerTcp> *valve) {
        BrokerTcp *broker = new BrokerTcp(loop);
        BrokerInfo info;
        // { group:dev1 ip:10.86.37.160 port:20881 room:cn0 timestamp:1572469298000 state:1 }

        info.addr = "10.86.37.160:20881";
        info.brokerGroupName = "dev1";
        info.ip = "10.86.37.160";
        info.port = 20881;
        info.room = "cn0";
        info.updateTimestamp = 1572469298000;
        info.state = 1;

        broker->setInfo(info);
        broker->setConnectCallback([valve](BrokerTcp *b) {
            cout << "BrokerConnected" << endl;
            valve->setTarget(b);
        });
        broker->setTerminateCallback([valve](BrokerTcp *b, int code) {
            cout << "BrokerTerminated" << endl;
            valve->clearTarget();
        });
        broker->setExile(5000);
        broker->connect();
    };

    auto callback = [](BrokerTcp *b) {
        uv_loop_t *loop = uv_default_loop();
        uv_timer_t *timer = new uv_timer_t;
        uv_timer_init(loop, timer);
        timer->data = b;
        uv_timer_start(
            timer,
            [](uv_timer_t *handle) {
                BrokerTcp *b = (BrokerTcp *)handle->data;
                b->heartbeat();
            },
            2000,
            2000);
    };

    valve.start(task, callback, 2000);

    uv_run(loop, UV_RUN_DEFAULT);
    return 0;
}