namespace Qunar.TC.Qmq.Client.Transport
{
    interface Sender
    {
        void Send(ProducerMessageImpl message);
    }
}

