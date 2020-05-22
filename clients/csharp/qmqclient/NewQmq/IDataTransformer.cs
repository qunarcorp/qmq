using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Codec.NewQmq;
using Qunar.TC.Qmq.Client.Metainfo;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    internal interface IDataTransformer
    {
        Datagram TransformRequest(Request request);

        Response TransformResponse(BrokerGroup brokerGroup, Datagram datagram);
    }
}
