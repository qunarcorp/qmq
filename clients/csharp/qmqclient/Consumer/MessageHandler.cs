// yuzhaohui
// 2016/7/19
using System.Threading;
using System.Collections.Concurrent;

namespace Qunar.TC.Qmq.Client.Consumer
{
    internal class MessageHandler
    {
        private const int MinThreads = 2;
        private const int MaxThreads = 5;

        private readonly BlockingCollection<IMessageHandleTask> _queue;
        private readonly MessageListener _listener;

        private int _currentTasks;

        public MessageHandler(MessageListener listener)
        {
            _queue = new BlockingCollection<IMessageHandleTask>(1000);
            _listener = listener;
        }

        public bool Handle(IMessageHandleTask task)
        {
            if (Enqueue(task, MinThreads)) return true;

            if (_queue.TryAdd(task)) return true;

            return Enqueue(task, MaxThreads);
        }

        private bool Enqueue(IMessageHandleTask task, int limit)
        {
            while (_currentTasks < limit)
            {
                var temp = _currentTasks;
                if (Interlocked.CompareExchange(ref _currentTasks, temp + 1, temp) != temp) continue;

                ThreadPool.QueueUserWorkItem(state =>
                {
                    task.Execute(_listener);

                    while (_queue.TryTake(out var item))
                    {
                        item.Execute(_listener);
                    }

                    Interlocked.Decrement(ref _currentTasks);
                });

                return true;
            }

            return false;
        }
    }
}
