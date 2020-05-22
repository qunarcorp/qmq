// yuzhaohui
// 12/10/2016

namespace Qunar.TC.Qmq.Client.Transport
{
    class AbstractHandler : Handler
    {
        public virtual void Close(HandlerContext context)
        {
            context.Close();
        }

        public virtual void InActive(HandlerContext context)
        {
            context.InActive();
        }

        public virtual void Receive(HandlerContext context, object msg)
        {
            context.Receive(msg);
        }

        public virtual void Write(HandlerContext context, object msg)
        {
            context.Write(msg);
        }
    }
}
