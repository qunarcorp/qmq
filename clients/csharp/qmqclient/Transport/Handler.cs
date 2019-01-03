// yuzhaohui
// 2016/7/14

namespace Qunar.TC.Qmq.Client.Transport
{
    interface Handler
    {
        // ->
        void Receive(HandlerContext context, object msg);

        // ->
        void InActive(HandlerContext context);

        // <-
        void Write(HandlerContext context, object msg);

        // <-
        void Close(HandlerContext context);
    }
}

