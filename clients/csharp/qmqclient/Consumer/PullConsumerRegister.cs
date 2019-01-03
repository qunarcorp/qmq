using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Qunar.TC.Qmq.Client.Dubbo;
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.Pull;

namespace Qunar.TC.Qmq.Client.Consumer
{
    internal class PullConsumerRegister : IConsumerRegister
    {
        private readonly string _appCode;
        private readonly string _metaServer;
        private readonly IRequestHandler _handler;

        //一个应用，相同的consumer实例只允许创建一个
        private readonly static ConcurrentDictionary<string, Puller> PULLERS = new ConcurrentDictionary<string, Puller>();

        public PullConsumerRegister(string appCode, string metaServer, IRequestHandler handler)
        {
            _appCode = appCode;
            _metaServer = metaServer;
            _handler = handler;
        }

        public void Registe(string subject, string group, ExtraListenerConfig config)
        {
            var consumer = PullConsumers.DefaultConsumer(subject, group, config.Mode, _appCode, _metaServer);
            var puller = new Puller((DefaultPullConsumer)consumer, config, _handler);
            if (PULLERS.TryAdd($"{subject}/{group}", puller))
            {
                puller.Start();
            }
        }

        public void Unregiste(string subject, string @group)
        {
            if (PULLERS.TryRemove($"{subject}/{group}", out var puller))
            {
                puller.Stop();
            }
        }

        private class Puller
        {
            private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();

            private readonly DefaultPullConsumer _consumer;
            private readonly ExtraListenerConfig _config;
            private readonly IRequestHandler _handler;

            private volatile Thread _pullThread;
            private volatile bool _stopped = false;

            public Puller(DefaultPullConsumer consumer, ExtraListenerConfig config, IRequestHandler handler)
            {
                _consumer = consumer;
                _config = config;
                _handler = handler;
            }

            public void Start()
            {
                _stopped = false;
                _pullThread = new Thread(_ =>
                {
                    PullUntilStopped();
                })
                {
                    IsBackground = true,
                    Name = $"qmq-puller-thread-${_consumer.Subject}/${_consumer.ConsumeMode}"
                };
                _pullThread.Start();
            }

            private void PullUntilStopped()
            {
                while (!_stopped)
                {
                    try
                    {
                        PullAndHandle();
                    }
                    catch (Exception e)
                    {
                        Logger.Error(e, $"unexpected exception. subject: {_consumer.Subject}, group: {_consumer.ConsumerGroup}");
                    }
                }
            }

            private void PullAndHandle()
            {
                try
                {
                    var begin = DateTime.Now;
                    var task = _consumer.AsyncPull(_config.PullBatchSize, _config.PullBatchTimeout);

                    HandleMessages(task.Result);
                }
                catch (AggregateException e)
                {
                    if (e.InnerException == null)
                    {
                        Logger.Error(e, $"consume newqmq message failed with unknown exception. subject: {_consumer.Subject}, group: {_consumer.ConsumerGroup}");
                    }
                    else
                    {
                        HandleException(e.InnerException);
                    }
                }
                catch (Exception e)
                {
                    HandleException(e);
                }
                finally
                {
                }
            }

            private void HandleMessages(IEnumerable<Message> messages)
            {
                foreach (var message in messages)
                {
                    HandleMessage(message);
                }
            }

            private void HandleMessage(Message message)
            {
                while (true)
                {
                    var result = _handler.Handle(message);
                    switch (result)
                    {
                        case ConsumerRejectException _:
                            Thread.Sleep(TimeSpan.FromSeconds(1));
                            break;
                        case Exception e:
                            Logger.Error(e, $"unexpected exception ocurred when process qmq message. subject: {_consumer.Subject}, group: {_consumer.ConsumerGroup}, messageId: {message.MessageId}");
                            break;
                        default:
                            return;
                    }
                }
            }

            private void HandleException(Exception e)
            {
                switch (e)
                {
                    case BrokerUnassignedException _:
                        var wait = TimeSpan.FromSeconds(10);
                        Thread.Sleep(wait);
                        break;
                    case NoWritableBrokerException _:
                        Logger.Error(e, $"cannot find a valid qmq broker to pull messages for {_consumer.Subject}/{_consumer.ConsumerGroup}.");
                        Thread.Sleep(TimeSpan.FromSeconds(5));
                        break;
                    //timeout的日志在之前已经打印了
                    case TimeoutException _:
                        break;
                    default:
                        Logger.Error(e, $"consume newqmq message failed. subject: {_consumer.Subject}, group: {_consumer.ConsumerGroup}");
                        break;
                }
            }

            public void Stop()
            {
                _stopped = true;
                _pullThread.Join();
            }
        }
    }
}