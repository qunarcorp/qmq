// yuzhaohui
// 12/4/2016
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using NLog;

namespace Qunar.TC.Qmq.Client.Util
{
    internal class BatchExecutor<Item> : IDisposable where Item : class
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        private readonly string _name;

        private readonly BlockingCollection<Action> _workQueue;
        private readonly BlockingCollection<Holder<Item>> _queue;

        private readonly int _threads;
        private readonly Thread[] _threadList;
        private readonly Action _task;

        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly CancellationToken _cancellationToken;

        private volatile bool _isStop = false;
        private volatile bool _isDisposed = false;

        public BatchExecutor(string name, int batchSize, Action<List<Item>> processor, int threads)
        {
            _name = name;
            _threads = threads;
            _cancellationTokenSource = new CancellationTokenSource();
            _cancellationToken = this._cancellationTokenSource.Token;

            _queue = new BlockingCollection<Holder<Item>>(10000);

            _task = () =>
            {
                while (_queue.Count > 0)
                {
                    var batch = new List<Item>();
                    for (var i = 0; i < batchSize; ++i)
                    {
                        _queue.TryTake(out var holder);
                        if (holder == null) break;

                        var item = holder.value;
                        holder.value = null;
                        batch.Add(item);
                    }

                    processor(batch);
                }
            };
            _workQueue = new BlockingCollection<Action>(threads);
            _threadList = new Thread[threads];
            StartThreads();
            Logger.Info("BatchExecutor inited");
        }

        private void StartThreads()
        {
            for (var i = 0; i < _threads; ++i)
            {
                _threadList[i] = new Thread(obj =>
                {
                    while (!_isStop)
                    {
                        try
                        {
                            var task = Take();
                            task?.Invoke();
                        }
                        catch
                        {
                        }
                    }
                })
                {
                    IsBackground = true,
                    Name = _name + "_" + i
                };
                _threadList[i].Start();
            }
        }

        private Action Take()
        {
            try
            {
                return _workQueue.Take(_cancellationToken);
            }
            catch
            {
                return null;
            }
        }

        public bool Add(Item item)
        {
            var add = _queue.TryAdd(new Holder<Item>(item));
            if (add)
            {
                _workQueue.TryAdd(_task);
            }

            return add;
        }

        public bool Add(Item item, int mills)
        {
            var add = _queue.TryAdd(new Holder<Item>(item), mills);
            if (add)
            {
                _workQueue.TryAdd(_task);
            }

            return add;
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                this._isStop = true;
                _cancellationTokenSource.Cancel(false);
            }
            _isDisposed = true;
        }

        private class Holder<T>
        {
            public T value;

            public Holder(T value)
            {
                this.value = value;
            }
        }
    }
}
