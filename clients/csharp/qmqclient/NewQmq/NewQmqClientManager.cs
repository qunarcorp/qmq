using System.Collections.Concurrent;
using Qunar.TC.Qmq.Client.Metainfo;
using Qunar.TC.Qmq.Client.Util;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class NewQmqClientManager
    {
        private readonly IDataTransformer _transformer;
        private readonly ConcurrentDictionary<string, NewQmqClient> _clients;

        public NewQmqClientManager(IDataTransformer transformer)
        {
            _transformer = transformer;
            _clients = new ConcurrentDictionary<string, NewQmqClient>();
        }

        public NewQmqClient GetOrCreate(BrokerGroup brokerGroup)
        {
            _clients.TryGetValue(brokerGroup.Master, out NewQmqClient client);
            if (client != null)
            {
                client.WaitAvailable();
                return client;
            }

            var newClient = Create(brokerGroup);
            var old = _clients.GetOrAdd(brokerGroup.Master, newClient);

            if (newClient == old)
            {
                newClient.Connect();
                newClient.WaitAvailable();
                return newClient;
            }

            old.WaitAvailable();
            return old;
        }

        private NewQmqClient Create(BrokerGroup brokerGroup)
        {
            var client = new NewQmqClient(brokerGroup, _transformer);

            client.OnStateChanged += (sender, args) =>
            {
                if (args.State == State.Close)
                {
                    _clients.TryRemove(brokerGroup.Master, out NewQmqClient temp);
                }
            };
            return client;
        }
    }
}