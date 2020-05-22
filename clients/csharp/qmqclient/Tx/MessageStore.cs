namespace Qunar.TC.Qmq.Client.Tx
{
    public interface MessageStore
    {
        void Save(ProducerMessage message);

        void Finish(ProducerMessage message);

        void Error(ProducerMessage message, int status);
    }
}