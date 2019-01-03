using NLog;
using Qunar.TC.Qmq.Client.Cluster;
using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Util;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.Metainfo;

namespace Qunar.TC.Qmq.Client.Transport
{
    internal class QueueSender : Sender, IDisposable
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        private static readonly ConcurrentDictionary<string, ICluster> Clusters = new ConcurrentDictionary<string, ICluster>();

        private readonly BatchExecutor<ProducerMessageImpl> _queue;

        private readonly BrokerGroupService _brokerGroupService;

        public QueueSender(BrokerGroupService brokerGroupService)
        {
            _brokerGroupService = brokerGroupService;
            _queue = new BatchExecutor<ProducerMessageImpl>("qmq-sender", 30, DoSend, 4);
        }

        public void Send(ProducerMessageImpl message)
        {
            try
            {

                if (_queue.Add(message))
                {

                }
                else if (_queue.Add(message, 50))
                {

                }
                else
                {
                    Failed(message, new EnqueueException(message.Base.MessageId));
                }
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
                throw;
            }
            finally
            {
            }
        }

        private void DoSend(List<ProducerMessageImpl> batch)
        {
            try
            {
                var group = Group(batch);
                var futures = new List<Tuple<Future<Response>, List<ProducerMessageImpl>>>();
                foreach (var item in group)
                {
                    var cluster = item.Key;
                    cluster.WaitAvailable(1000);
                    var request = new Request(item.Value.Item1);
                    var result = cluster.Send(request);
                    futures.Add(Tuple.Create(result, item.Value.Item2));
                }

                ProcessResult(futures);
            }
            catch (Exception ex)
            {
            }
            finally
            {
            }
        }

        private IDictionary<ICluster, Tuple<List<Message>, List<ProducerMessageImpl>>> Group(IEnumerable<ProducerMessageImpl> messages)
        {
            var group = new Dictionary<ICluster, Tuple<List<Message>, List<ProducerMessageImpl>>>();
            foreach (var m in messages)
            {
                var message = m.Base;
                var cluster = Resolve(message);

                @group.TryGetValue(cluster, out var batch);
                if (batch == null)
                {
                    batch = Tuple.Create(new List<Message>(), new List<ProducerMessageImpl>());
                    @group[cluster] = batch;
                }
                batch.Item1.Add(message);
                batch.Item2.Add(m);
            }

            return group;
        }

        private ICluster Resolve(Message message)
        {
            bool isDelayMessage = IsDelayMessage(message);
            string key = message.Subject + "|" + isDelayMessage;
            if (Clusters.TryGetValue(key, out ICluster cluster))
            {
                return cluster;
            }

            var newCluster = new NewQmqCluster(key, _brokerGroupService, message.Subject, isDelayMessage);
            var oldCluster = Clusters.GetOrAdd(key, newCluster);
            if (oldCluster == newCluster)
            {
                newCluster.Init();
            }
            return oldCluster;
        }

        private bool IsDelayMessage(Message message)
        {
            var schedule = message.GetDateProperty(BaseMessage.keys.qmq_scheduleRecevieTime.ToString());
            if (schedule != null)
            {
                return true;
            }

            return false;
        }

        private void ProcessResult(IEnumerable<Tuple<Future<Response>, List<ProducerMessageImpl>>> futures)
        {
            foreach (var result in futures)
            {
                try
                {
                    if (result.Item1 == null)
                    {
                        Failed(result.Item2, null);
                        continue;
                    }
                    var res = result.Item1.Result;
                    if (res == Response.Timeout)
                    {
                        Failed(result.Item2, (TimeoutException)res.Result);
                    }
                    else if (res.Status != Response.Ok)
                    {
                        Failed(result.Item2, (Exception)res.Result);
                    }
                    else
                    {
                        ProcessResult((Hashtable)res.Result, result.Item2);
                    }
                }
                catch (Exception e)
                {
                    Failed(result.Item2, e);
                }
            }
        }

        private static void ProcessResult(IDictionary result, IEnumerable<ProducerMessageImpl> batch)
        {
            if (result.Count == 0)
            {
                Success(batch);
                return;
            }

            var success = new List<ProducerMessageImpl>();
            foreach (var pm in batch)
            {
                var message = pm.Base;
                var exception = result[message.MessageId] as Exception;
                if (exception != null)
                {
                    Failed(pm, exception);
                }
                else
                {
                    success.Add(pm);
                }
            }

            Success(success);
        }

        private static void Success(IEnumerable<ProducerMessageImpl> list)
        {
            foreach (var pm in list)
            {
                Success(pm);
            }
        }

        private static void Success(ProducerMessageImpl message)
        {
            message.Success();
        }

        private static void Failed(ProducerMessageImpl message, Exception exception)
        {
            if (exception is DuplicateMessageException)
            {
                Success(message);
                return;
            }

            message.Failed(exception);
        }

        private static void Failed(IEnumerable<ProducerMessageImpl> messages, Exception exception)
        {
            foreach (var message in messages)
            {
                message.Failed(exception);
            }
        }

        public void Dispose()
        {
            _queue.Dispose();
        }
    }
}

