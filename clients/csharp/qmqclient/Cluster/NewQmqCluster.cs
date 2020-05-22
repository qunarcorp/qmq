using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Metainfo;
using Qunar.TC.Qmq.Client.NewQmq;
using Qunar.TC.Qmq.Client.Transport;
using Qunar.TC.Qmq.Client.Util;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Qunar.TC.Qmq.Client.Cluster
{
    internal class NewQmqCluster : ICluster
    {
        private static readonly NLog.Logger LOG = NLog.LogManager.GetCurrentClassLogger();

        private static readonly NewQmqClientManager ClientManager = new NewQmqClientManager(new SendMessageDataTransformer());

        private readonly BrokerGroupService _brokerGroupService;

        private readonly string _subject;
        private readonly bool _isDelayMessage;

        private volatile bool _avaliable;
        private readonly ManualResetEventSlim _waitAvaliable;

        public NewQmqCluster(string name, BrokerGroupService brokerGroupService, string subject, bool isDelayMessage)
        {
            _waitAvaliable = new ManualResetEventSlim();
            Name = name;
            _subject = subject;
            _isDelayMessage = isDelayMessage;

            _brokerGroupService = brokerGroupService;
        }

        public string Name { get; }

        public void Init()
        {
            InitNewQmqClients();
        }

        public void Broadcast(Request request)
        {
            throw new NotSupportedException("NewQmq cluster doesn't support broadcast request.");
        }

        public Future<Response> Send(Request request)
        {
            var tuple = SelectWritableClient();
            if (tuple == null)
            {
                InitNewQmqClients(true);
                return new Future<Response>(3000)
                {
                    Source = request,
                    Result = new Response(-1, Response.Error)
                    {
                        Result = new NoWritableBrokerException($"Cannot find writable broker for subject {_subject} to send messages.")
                    }
                };
            }

            // TODO(keli.wang): make timeout configable
            var future = new Future<Response>(8000) { Source = request };
            tuple.Item2.Send(request, future.timeout)
                .ContinueWith(task =>
                {
                    if (!task.Result.IsOk())
                    {
                        LOG.Error($"Send message subject: {_subject}, to {tuple.Item1.Name}({tuple.Item1.Master}) failed, error: {task.Result.ErrorMessage}");
                        tuple.Item1.BrokerState = BrokerState.Nrw;
                    }
                    future.Result = task.Result;
                }, TaskScheduler.Default);
            return future;
        }

        public void WaitAvailable(int mills)
        {
            if (_avaliable)
                return;
            _waitAvaliable.Wait(mills);
        }

        private void InitNewQmqClients(bool forceLoad = false)
        {
            var clusterInfo = _isDelayMessage ? _brokerGroupService.DelayProducerGetSubjectCluster(_subject, forceLoad) : _brokerGroupService.ProducerGetSubjectCluster(_subject, forceLoad);
            foreach (var brokerGroup in clusterInfo.BrokerGroups.Values)
                ClientManager.GetOrCreate(brokerGroup);
            _avaliable = true;
            _waitAvaliable.Set();
        }

        private Tuple<BrokerGroup, NewQmqClient> SelectWritableClient()
        {
            var cluster = _isDelayMessage ? _brokerGroupService.DelayProducerGetSubjectCluster(_subject) : _brokerGroupService.ProducerGetSubjectCluster(_subject);
            if (cluster.BrokerGroups.Count == 0)
            {
                return null;
            }

            var brokerGroups = DictValueFilter.Filter(cluster.BrokerGroups, BrokerGroup.IsWritable);
            if (brokerGroups.Count == 0)
            {
                return null;
            }

            // First of all, random select a client.
            // If random selected client is not writable, then select first writable client.
            var randBrokerGroup = brokerGroups.ElementAt(StaticRandom.NextRand(brokerGroups.Count));
            var randClient = ClientManager.GetOrCreate(randBrokerGroup);
            if (randClient.Writtable)
                return new Tuple<BrokerGroup, NewQmqClient>(randBrokerGroup, randClient);
            var client = brokerGroups.Select(brokerGroup => new Tuple<BrokerGroup, NewQmqClient>(brokerGroup, ClientManager.GetOrCreate(brokerGroup)))
                .FirstOrDefault(tuple => tuple.Item2.Writtable);
            return client;
        }

        public override bool Equals(object obj)
        {
            switch (obj)
            {
                case NewQmqCluster cluster:
                    return Name.Equals(cluster.Name);
                default:
                    return false;
            }
        }

        public override int GetHashCode()
        {
            return 1;
        }
    }
}