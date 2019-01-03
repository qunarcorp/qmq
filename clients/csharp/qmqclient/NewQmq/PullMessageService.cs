using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Metainfo;
using Qunar.TC.Qmq.Client.NewQmq.Model;
using System;
using System.Threading.Tasks;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal class PullMessageService
    {
        private const int MinRoundtripMs = 8000;

        private readonly AckHandlerQueueManager _ackHandlerQueueManager;
        private readonly NewQmqClientManager _clientManager;

        public PullMessageService(AckHandlerQueueManager ackHandlerQueueManager)
        {
            _ackHandlerQueueManager = ackHandlerQueueManager;
            _clientManager = new NewQmqClientManager(new PullMessageDataTransformer());
        }

        public Task<Response> Pull(BrokerGroup group, PullMessageRequest request)
        {
            var client = CreateOrGetClient(group);
            if (client == null)
            {
                return ErrorTask(new NoWritableBrokerException($"Cannot find pullable broker for {request.Subject}/{request.Group}"));
            }

            var ackHandlerQueue = _ackHandlerQueueManager.GetOrCreate(request.Subject, request.Group, client.BrokerGroupName, request.IsBroadcast);
            request.PullOffsetBegin = ackHandlerQueue.MinPullOffset;
            request.PullOffsetEnd = ackHandlerQueue.MaxPullOffset;
            var timeout = request.TimeoutMillis < 0 ? MinRoundtripMs : (request.TimeoutMillis + MinRoundtripMs);
            return client.Send(new Request(request), timeout);
        }

        private static Task<Response> ErrorTask(Exception e)
        {
            var tsc = new TaskCompletionSource<Response>();
            tsc.TrySetResult(new Response(-1, Response.Error)
            {
                Result = e
            });
            return tsc.Task;
        }

        private NewQmqClient CreateOrGetClient(BrokerGroup group)
        {
            var randClient = _clientManager.GetOrCreate(group);
            if (randClient.Writtable)
            {
                return randClient;
            }

            return null;
        }
    }
}