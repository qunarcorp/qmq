// yuzhaohui
// 11/25/2016
namespace Qunar.TC.Qmq.Client
{
    internal interface MessageSendStateListener
    {
        void OnSuccess(Message message);

        void OnFailed(Message message);
    }
}
