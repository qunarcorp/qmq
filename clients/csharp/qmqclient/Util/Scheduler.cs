// yuzhaohui
// 11/28/2016
using System;
using System.Threading;
using System.Collections.Generic;

namespace Qunar.TC.Qmq.Client.Util
{
    class Scheduler
    {
        public static readonly Scheduler INSTANCE = new Scheduler();

        public delegate void Callback();

        private readonly Timer timer;

        private readonly PriorityQueue<DelayTask> queue;

        public Scheduler()
        {
            this.queue = new PriorityQueue<DelayTask>();
            this.timer = new Timer((state) =>
            {
                var list = new List<DelayTask>();
                lock (this.queue)
                {
                    while (queue.Count != 0)
                    {
                        var task = queue.Peek();
                        if (task == null) return;

                        if (DateTime.Now >= task.deadline)
                        {
                            queue.Dequeue();
                            if (task.IsCanceled) continue;
                            list.Add(task);
                        }
                        else
                        {
                            break;
                        }
                    }
                }

                foreach (var item in list)
                {
                    try
                    {
                        item.callback();
                    }
                    catch { }
                    NextSchedule(item);
                }
            }, null, 0, 500);
        }

        private void NextSchedule(DelayTask item)
        {
            if (item.interval <= 0) return;
            item.deadline = DateTime.Now.AddSeconds(item.interval);
            lock (this.queue)
            {
                this.queue.Enqueue(item);
            }
        }

        public void Schedule(Callback callback, DateTime deadline)
        {
            var task = new DelayTask();
            task.deadline = deadline;
            task.callback = callback;
            lock (this.queue)
            {
                this.queue.Enqueue(task);
            }
        }

        public DelayTask ScheduleAtFixDelay(Callback callback, int interval)
        {
            var task = new DelayTask();
            task.deadline = DateTime.Now.AddSeconds(interval);
            task.interval = interval;
            task.callback = callback;
            lock (this.queue)
            {
                this.queue.Enqueue(task);
            }

            return task;
        }
    }

    class DelayTask : IComparable<DelayTask>
    {
        private volatile bool isCanceled = false;

        public DateTime deadline;

        public Scheduler.Callback callback;

        public int interval;

        public int CompareTo(DelayTask other)
        {
            return deadline.CompareTo(other.deadline);
        }

        public void Cancel()
        {
            isCanceled = true;
        }

        public bool IsCanceled
        {
            get { return isCanceled; }
        }
    }
}
