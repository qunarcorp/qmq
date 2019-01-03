using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Transport;

namespace Qunar.TC.Qmq.Client.Cluster
{
    internal interface ICluster
    {
        string Name { get; }

        void Init();

        Future<Response> Send(Request request);

        void Broadcast(Request request);

        void WaitAvailable(int mills);
    }
}

