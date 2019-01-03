// yuzhaohui
// 2016/7/15

namespace Qunar.TC.Qmq.Client.Consumer
{
    internal interface IConsumerRegister
    {
        void Registe(string prefix, string group, ExtraListenerConfig config);

        void Unregiste(string prefix, string group);
    }
}
