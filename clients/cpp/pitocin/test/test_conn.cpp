#include "Conn.hpp"
#include <iostream>
#include <uv.h>
#include <sstream>

int main()
{
    using namespace std;
    uv_loop_t *loop = uv_default_loop();
    Pitocin::Conn conn(loop);
    conn.setAddr("127.0.0.1", 8080);
    conn.setOnConnectCallback([&conn]() {
        cout << "on_connect" << endl;
        stringstream s;
        s << "GET /meta/address HTTP/1.1"
          << "\r\n";
        s << "Host: 127.0.0.1"
          << "\r\n";
        s << "Connection: close"
          << "\r\n";
        s << "\r\n";
        conn.write(s.str());
    });
    conn.setOnDisconnectCallback([]() {
        cout << "on_disconnect" << endl;
    });
    conn.setReadCallback([](char *buf, size_t len) {
        string data(buf, len);
        // cout << "< size:" << len << endl;
        cout << data << endl;
    });
    conn.connect();
    uv_run(loop, UV_RUN_DEFAULT);
    return 0;
}
