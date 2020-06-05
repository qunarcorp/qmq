#ifndef PITOCIN_REQUEST_VALVE_H
#define PITOCIN_REQUEST_VALVE_H

/**
 * 该class是为了解决这个场景的问题:
 *   当调用一个请求时,需要首先建立一个tcp的长链接, 而建立链接是一个io操作需要一定时间,
 * 如果在这段时间内并发了多个请求, 为了避免大家都去创建一个链接, 就需要有一个机制让所有
 * 请求在第一个建立链接的操作上等待, 然后服用该链接. 
 */

#include <functional>
#include <vector>
#include <uv.h>
#include <iostream>

using namespace std;

namespace Pitocin
{
template <typename T>
class RequestValve
{
    uv_loop_t *loop;
    T *target = nullptr;
    uv_timer_t timer;
    bool timerRunning = false;
    vector<function<void(T *)>> reqCache;

public:
    RequestValve() = delete;
    RequestValve(uv_loop_t *loop)
        : loop(loop)
    {
    }
    ~RequestValve()
    {
        if (timerRunning)
        {
            uv_timer_stop(&timer);
            timerRunning = false;
            done();
        }
        clearTarget();
    }
    void setTarget(T *tar)
    {
        if (tar != nullptr)
        {
            if ((target != nullptr) && (target != tar))
            {
                delete target;
                target = nullptr;
            }
            target = tar;
            done();
        }
    }
    T *getTarget()
    {
        return target;
    }
    void clearTarget()
    {
        if (target != nullptr)
        {
            delete target;
            target = nullptr;
        }
    }
    void start(function<void(RequestValve *)> task, function<void(T *)> callback, uint64_t timeout)
    {
        if (target == nullptr)
        {
            if (callback)
            {
                reqCache.push_back(callback);
            }
            if (!timerRunning)
            {
                uv_timer_init(loop, &timer);
                timer.data = this;
                uv_timer_start(
                    &timer,
                    [](uv_timer_t *handle) {
                        RequestValve *self = (RequestValve *)handle->data;
                        self->timerRunning = false;
                        if (self->target == nullptr)
                        {
                            self->done();
                        }
                    },
                    timeout,
                    0);
                timerRunning = true;
                if (task)
                {
                    task(this);
                }
            }
        }
        else
        {
            if (callback)
            {
                callback(target);
            }
        }
    }

private:
    void done()
    {
        for (auto callback : reqCache)
        {
            callback(target);
        }
        reqCache.clear();
    }
};

} // namespace Pitocin

#endif //PITOCIN_REQUEST_VALVE_H