// yuzhaohui
// 2016/7/21
namespace Qunar.TC.Qmq.Client.Transport
{
    internal interface Processor
    {
        void Process(HandlerContext context, object msg);
    }
}

