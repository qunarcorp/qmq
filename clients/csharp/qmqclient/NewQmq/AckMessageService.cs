using System.Threading.Tasks;
using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Metainfo;
using Qunar.TC.Qmq.Client.NewQmq.Model;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class AckMessageService
    {
        private readonly BrokerGroupService _brokerGroupService;
        private readonly NewQmqClientManager _clientManager;

        public AckMessageService(BrokerGroupService brokerGroupService)
        {
            _brokerGroupService = brokerGroupService;
            _clientManager = new NewQmqClientManager(new AckMessageDataTransformer());
        }

        public Task<Response> Ack(string brokerGroup, AckRequest request)
        {
            var client = GetBrokerGroupClient(brokerGroup, request.Subject, request.Group);
            if (client == null || !client.Writtable)
            {
                _brokerGroupService.ConsumerGetSubjectCluster(request.Subject, request.Group, true);
                var tsc = new TaskCompletionSource<Response>();
                tsc.SetResult(new Response(-1, Response.Error)
                {
                    Result = new NoWritableBrokerException(
                        $"broker group {brokerGroup} is not writable now, so cannot send ack for {request.Subject}/{request.Group}")
                });
                return tsc.Task;
            }

            return client.Send(new Request(request), 3000);
        }

        private NewQmqClient GetBrokerGroupClient(string brokerGroupName, string subject, string group)
        {
            var brokerGroups = _brokerGroupService.ConsumerGetSubjectCluster(subject, group).BrokerGroups;
            if (brokerGroups.Count == 0 || !brokerGroups.ContainsKey(brokerGroupName))
            {
                return null;
            }

            var brokerGroup = brokerGroups[brokerGroupName];
            if (brokerGroup == null || !BrokerGroup.IsReadable(brokerGroup))
            {
                return null;
            }

            return _clientManager.GetOrCreate(brokerGroup);
        }
    }
}