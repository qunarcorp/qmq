namespace Qunar.TC.Qmq.Client.Transport
{
    internal interface IKeepliveStrategy
    {
        void CheckStatus(TransportClient client, long lastWrite, long lastRead, long lastConnect);
    }
}
