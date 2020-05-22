using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Metainfo;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.Util;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class SendMessageBackService
    {
        private readonly BrokerGroupService _brokerGroupService;
        private readonly NewQmqClientManager _clientManager;

        public SendMessageBackService(BrokerGroupService brokerGroupService)
        {
            _brokerGroupService = brokerGroupService;
            _clientManager = new NewQmqClientManager(new SendMessageDataTransformer());
        }

        public Task<Response> SendBack(string subject, BaseMessage message, bool forceRefreshBrokerGroup = false, bool isDelayMessage = false)
        {
            var request = new Request(new List<Message> { message });
            var client = SelectWritableClient(subject, forceRefreshBrokerGroup, isDelayMessage);
            return client == null ? ErrorTask(subject) : client.Send(request, 3000);
        }

        private static Task<Response> ErrorTask(string subject)
        {
            var tsc = new TaskCompletionSource<Response>();
            tsc.TrySetResult(new Response(-1, Response.Error)
            {
                Result = new NoWritableBrokerException(
                    $"Cannot find writable broker for subject {subject} to send message back.")
            });
            return tsc.Task;
        }

        private NewQmqClient SelectWritableClient(string subject, bool forceRefreshBrokerGroup, bool isDelayMessage)
        {
            var cluster = isDelayMessage ? _brokerGroupService.DelayProducerGetSubjectCluster(subject, forceRefreshBrokerGroup) : _brokerGroupService.ProducerGetSubjectCluster(subject, forceRefreshBrokerGroup);
            if (cluster.BrokerGroups.Count == 0)
                return null;

            var brokerGroups = cluster.BrokerGroups.Values.Where(BrokerGroup.IsWritable).ToList();
            if (brokerGroups.Count == 0)
            {
                return null;
            }

            // First of all, random select a client.
            // If random selected client is not writable, then select first writable client.
            var randBrokerGroup = brokerGroups.ElementAt(StaticRandom.NextRand(brokerGroups.Count));
            var randClient = _clientManager.GetOrCreate(randBrokerGroup);
            if (randClient.Writtable)
                return randClient;
            return brokerGroups.Select(brokerGroup => _clientManager.GetOrCreate(brokerGroup))
                .FirstOrDefault(client => client.Writtable);
        }
    }
}