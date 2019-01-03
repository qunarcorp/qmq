// yuzhaohui
// 2016/7/14

namespace Qunar.TC.Qmq.Client
{
    public delegate void MessageListener(Message message);

    public interface MessageConsumer
    {
        MessageSubscriber Subscribe(string prefix, string group);
    }
}
