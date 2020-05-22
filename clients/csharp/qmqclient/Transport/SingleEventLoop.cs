using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Qunar.TC.Qmq.Client.Transport
{
    class SingleEventLoop
    {
        private static int index = 0;

        private readonly BlockingCollection<Action> _queue;

        private readonly Thread _executor;

        private volatile bool _isStop;

        public SingleEventLoop()
        {
            _queue = new BlockingCollection<Action>();
            _executor = new Thread(() =>
            {
                while (!_isStop)
                {
                    var action = _queue.Take();
                    if (action == null) continue;

                    try
                    {
                        action();
                    }
                    catch
                    {

                    }
                }
            })
            {
                Name = "Qmq-Network-EventLoop-" + Interlocked.Increment(ref index),
                IsBackground = true
            };
        }

        public void Start()
        {
            _executor.Start();
        }

        public void Execute(Action task)
        {
            _queue.Add(task);
        }

        public bool InEventLoop
        {
            get
            {
                return Thread.CurrentThread == _executor;
            }
        }

    }
}
