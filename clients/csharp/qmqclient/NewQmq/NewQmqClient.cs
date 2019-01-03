using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Codec.NewQmq;
using Qunar.TC.Qmq.Client.Metainfo;
using Qunar.TC.Qmq.Client.Transport;
using Qunar.TC.Qmq.Client.Util;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class NewQmqClient : Processor
    {
        private static readonly NLog.Logger LOG = NLog.LogManager.GetCurrentClassLogger();

        private static readonly ConcurrentDictionary<int, Promise> Promises = new ConcurrentDictionary<int, Promise>();

        private readonly BrokerGroup _brokerGroup;
        private readonly TransportClient _client;
        private readonly IDataTransformer _transformer;

        static NewQmqClient()
        {
            var checktimeoutThread = new Thread(_ =>
            {
                while (true)
                {
                    try
                    {
                        CheckTimeout();
                    }
                    catch
                    {
                        // ignored
                    }

                    Thread.Sleep(500);
                }
            })
            {
                Name = "newqmq-checktimeout-thread",
                IsBackground = true
            };
            checktimeoutThread.Start();
        }

        public event EventHandler<StateChangedArgs> OnStateChanged
        {
            add => _client.StateChanged += value;
            remove => _client.StateChanged -= value;
        }

        private readonly ManualResetEventSlim connected = new ManualResetEventSlim();

        public NewQmqClient(BrokerGroup brokerGroup, IDataTransformer transformer)
        {
            _brokerGroup = brokerGroup;
            _transformer = transformer;

            var arr = brokerGroup.Master.Split(':');
            _client = new TransportClient(arr[0], int.Parse(arr[1]),
                new NewQmqEncoder(), new NewQmqDecoder(),
                new NewQmqClientKeepliveStrategy(), false, 2 * 1024 * 1024)
            {
                Processor = this,
            };

            _client.StateChanged += (sender, args) =>
            {
                connected.Set();
            };
        }

        public string Host => _client.Host;

        public int Port => _client.Port;

        public string BrokerGroupName => _brokerGroup.Name;

        public bool Writtable => _client.Writtable;

        public bool IsClose => _client.IsClose;

        public void WaitAvailable()
        {
            if (Writtable) return;
            connected.Wait(1000);
        }

        void Processor.Process(HandlerContext context, object msg)
        {
            var datagram = msg as Datagram;

            if (datagram.Header.Code == CommandCode.Heartbeat)
            {
                return;
            }

            if (_transformer == null)
            {
                throw new Exception("transformer is null");
            }

            var response = _transformer.TransformResponse(_brokerGroup, datagram);
            var id = datagram.Header.Opaque;
            Promises.TryRemove(id, out var promise);
            if (promise != null)
            {
                promise.Source.TrySetResult(response);
            }
            else
            {
                LOG.Warn($"request {id} finally return");
            }

        }

        public void Close()
        {
            _client.Close();
        }

        public void Connect()
        {
            _client.Connect();
        }

        public Task<Response> Send(Request request, long timeout)
        {
            var tsc = new TaskCompletionSource<Response>();
            if (_transformer == null)
            {
                tsc.TrySetResult(new Response(-1, Response.Error)
                {
                    Result = new Exception("transformer is null")
                });
            }
            else
            {
                try
                {
                    var datagram = _transformer.TransformRequest(request);
                    Promises[datagram.Header.Opaque] = new Promise(tsc, timeout);
                    _client.Send(datagram);
                }
                catch (Exception e)
                {
                    tsc.TrySetResult(new Response(-1, Response.Error)
                    {
                        Result = e
                    });
                }
            }

            return tsc.Task;
        }

        private static void CheckTimeout()
        {
            foreach (var entry in Promises)
            {
                if (entry.Value == null || entry.Value.Source.Task.IsCompleted)
                {
                    continue;
                }

                if (DateTime.Now - entry.Value.Sent >= TimeSpan.FromMilliseconds(entry.Value.Timeout))
                {
                    Promises.TryRemove(entry.Key, out var promise);
                    if (promise == null) continue;

                    var res = new Response(entry.Key, Response.Timout)
                    {
                        Result = Response.TimeoutException,
                        ErrorMessage = $"request {entry.Key} is timeout, sent: {promise.Sent} now: {DateTime.Now}"
                    };
                    promise.Source.TrySetResult(res); ;
                }
            }
        }

        private class Promise
        {
            public Promise(TaskCompletionSource<Response> tsc, long timeout)
            {
                Sent = DateTime.Now;
                Source = tsc;
                Timeout = timeout;
            }

            public DateTime Sent { get; }

            public TaskCompletionSource<Response> Source { get; }

            public long Timeout { get; }
        }
    }
}