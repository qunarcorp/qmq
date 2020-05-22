using System;

namespace Qunar.TC.Qmq.Client.Transport
{
    class EventLoop
    {
        private SingleEventLoop[] workers;

        private int index = 0;

        public EventLoop(int workerCount)
        {
            workers = new SingleEventLoop[workerCount];
            for (var i = 0; i < workers.Length; ++i)
            {
                workers[i] = new SingleEventLoop();
                workers[i].Start();
            }
        }

        public SingleEventLoop NextWorker()
        {
            var count = workers.Length;
            return workers[Math.Abs((index++) % count)];
        }
    }
}
