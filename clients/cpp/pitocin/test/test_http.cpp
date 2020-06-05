#include "Http.hpp"
#include <uv.h>
#include <iostream>

int main()
{
    uv_loop_t *loop = uv_default_loop();
    Pitocin::Http http(loop);
    http.setUrl("http://127.0.0.1:8080/meta/address");
    http.setTimeout(2000);
    http.setCallback([](int code, std::string &&resp) {
        std::cout << "code:" << code << std::endl;
        if (code == 200)
        {
            std::cout << "resp:" << resp << std::endl;
        }
    });
    http.startGetRequest();
    uv_run(loop, UV_RUN_DEFAULT);
    return 0;
}