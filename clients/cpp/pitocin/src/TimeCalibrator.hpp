#ifndef PITOCIN_TIME_CALIBRATOR_H
#define PITOCIN_TIME_CALIBRATOR_H

#include <uv.h>
#include <ctime>
#include <string>

using namespace std;

namespace Pitocin
{
class TimeCalibrator
{
    uv_loop_t *loop;
    uint64_t gap;
    uv_timer_t timer;

public:
    TimeCalibrator() = delete;
    TimeCalibrator(uv_loop_t *loop)
        : loop(loop)
    {
        initGap();
        uv_timer_init(loop, &timer);
        timer.data = this;
        uv_timer_start(
            &timer,
            [](uv_timer_t *handle) {
                TimeCalibrator *self = (TimeCalibrator *)handle->data;
            },
            20000,
            20000);
    }
    ~TimeCalibrator()
    {
        uv_timer_stop(&timer);
    }

    string msgIdTime()
    {
        time_t t = (time_t)((uv_now(loop) / 1000) + gap);
        tm *timeInfo = localtime(&t);
        char buffer[128];
        size_t len = strftime(buffer, sizeof(buffer), "%y%m%d.%H%M%S", timeInfo);
        return string(buffer, len);
    }

    uint64_t seconds()
    {
        return (uv_now(loop) / 1000) + gap;
    }

    uint64_t milliseconds()
    {
        return uv_now(loop) + (gap * 1000);
    }

private:
    void initGap()
    {
        time_t t = time(nullptr);
        uint64_t u = uv_now(loop) / 1000;
        gap = (uint64_t)t - u;
    }
};
} // namespace Pitocin

#endif //PITOCIN_TIME_CALIBRATOR_H
