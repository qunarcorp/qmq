// yuzhaohui
// 2016/7/14
namespace Qunar.TC.Qmq.Client.Transport
{
    internal class HandlerContext
    {
        private readonly Handler handler;
        private readonly Channel channel;

        internal HandlerContext prev;
        internal HandlerContext next;

        public HandlerContext(Channel channel, Handler handler)
        {
            this.channel = channel;
            this.handler = handler;
        }

        public void Write(object msg)
        {
            prev.handler.Write(prev, msg);
        }

        public void Receive(object msg)
        {
            next.handler.Receive(next, msg);
        }

        public void Close()
        {
            prev.handler.Close(prev);
        }

        public void InActive()
        {
            next.handler.InActive(next);
        }

        public Channel Channel
        {
            get
            {
                return channel;
            }
        }
    }
}

