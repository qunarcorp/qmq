#ifndef PITOCIN_EXILED_CHECKER_H
#define PITOCIN_EXILED_CHECKER_H

#include <uv.h>
#include <cstdint>
#include <functional>

using namespace std;

namespace Pitocin
{
class ExiledChecker
{
    uv_loop_t *loop;
    bool hasRequest = false;
    uv_timer_t timer;
    uint64_t interval;
    function<void(uint64_t)> exiledCallback;

public:
    ExiledChecker() = delete;
    ExiledChecker(uv_loop_t *loop)
        : loop(loop)
    {
        uv_timer_init(loop, &timer);
    }
    ~ExiledChecker()
    {
        uv_timer_stop(&timer);
    }

    void setInterval(uint64_t t)
    {
        interval = t;
    }
    void setCallback(function<void(uint64_t timestamp)> callback)
    {
        exiledCallback = callback;
    }
    void start()
    {
        timer.data = this;
        uv_timer_start(
            &timer,
            [](uv_timer_t *handle) {
                ExiledChecker *self = (ExiledChecker *)handle->data;
                if (self->hasRequest)
                {
                    self->hasRequest = false;
                    return;
                }
                uv_timer_stop(&self->timer);
                uint64_t ts = uv_now(self->loop);
                self->exiledCallback(ts);
            },
            interval,
            interval);
    }
    void doRequest()
    {
        hasRequest = true;
    }
};
} // namespace Pitocin

#endif //PITOCIN_EXILED_CHECKER_H
