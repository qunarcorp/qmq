namespace Qunar.TC.Qmq.Client
{
    public interface ProducerMessage
    {
        Message Base
        {
            get;
        }

        object RouteKey
        {
            get;
            set;
        }
    }
}
