#include <uv.h>
#include <cstdint>
#include <string>
#include <iostream>
#include <ctime>
#include "TimeCalibrator.hpp"

using namespace std;

struct Time
{
    uint16_t year;
    uint8_t month;
    uint8_t day;
    uint8_t hour;
    uint8_t minute;
    uint8_t second;

    static string now()
    {
        time_t t;
        time(&t);
        cout << "ts:" << t << endl;
        char buffer[128];
        struct tm *timeInfo = localtime(&t);

        size_t len = strftime(buffer, sizeof(buffer), "%y%m%d.%H%M%S", timeInfo);
        return string(buffer, len);
    }
};

int main()
{
    uv_loop_t *loop = uv_default_loop();
    Pitocin::TimeCalibrator calibrator(loop);
    uv_timer_t timer;
    uv_timer_init(loop, &timer);
    timer.data = &calibrator;
    uv_timer_start(
        &timer,
        [](uv_timer_t *handle) {
            // cout << Time::now() << endl;
            // cout << "uv:" << +uv_now(uv_default_loop()) << endl;
            Pitocin::TimeCalibrator *calibrator = (Pitocin::TimeCalibrator *)handle->data;
            cout << calibrator->msgIdTime() << endl;
        },
        0,
        1000);
    uv_run(loop, UV_RUN_DEFAULT);
    return 0;
}
