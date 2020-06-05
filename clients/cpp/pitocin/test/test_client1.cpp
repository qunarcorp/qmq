#include "Client.hpp"
#include <uv.h>
#include <iostream>
#include <string>

int main()
{
    using namespace std;
    uv_loop_t *loop = uv_default_loop();
    cout << uv_version_string() << endl;
    Pitocin::Client client(loop);
    client.setErrCallback([](int code, string &&info) {
        cout << "ERR"
             << " code:" << code
             << " info:" << info << endl;
    });
    client.setHttpUrl("http://127.0.0.1:8080/meta/address");
    client.setHttpReqInterval(3000);
    client.start();
    uv_run(loop, UV_RUN_DEFAULT);
    return 0;
}