// yuzhaohui
// 12/10/2016

namespace Qunar.TC.Qmq.Client.Transport
{
    class ChannelPipeline
    {
        private readonly HandlerContext head;
        private readonly HandlerContext tail;

        private readonly Channel channel;

        public ChannelPipeline(Channel channel)
        {
            this.channel = channel;
            this.head = new HandlerContext(channel, new HeadHandler(this));
            this.tail = new HandlerContext(channel, new TailHandler());
            this.head.next = tail;
            this.tail.prev = head;
        }

        public Channel Channel()
        {
            return this.channel;
        }

        public void AddLast(Handler handler)
        {
            var newCtx = new HandlerContext(this.channel, handler);

            var pre = tail.prev;
            newCtx.prev = pre;
            newCtx.next = tail;
            pre.next = newCtx;
            tail.prev = newCtx;
        }

        public void Write(object msg)
        {
            this.tail.Write(msg);
        }

        public void Receive(object msg)
        {
            this.head.Receive(msg);
        }

        public void InActive()
        {
            this.head.InActive();
        }

        public class HeadHandler : Handler
        {
            private readonly ChannelPipeline pipeline;

            public HeadHandler(ChannelPipeline pipeline)
            {
                this.pipeline = pipeline;
            }

            public void Close(HandlerContext context)
            {
                pipeline.Channel().Close();
            }

            public void InActive(HandlerContext context)
            {
                context.InActive();
            }

            public void Receive(HandlerContext context, object msg)
            {
                context.Receive(msg);
            }

            public void Write(HandlerContext context, object msg)
            {
                if (!(msg is byte[])) return;
                pipeline.Channel().Write((byte[])msg);
            }
        }

        public class TailHandler : Handler
        {
            public void Close(HandlerContext context)
            {

            }

            public void InActive(HandlerContext context)
            {

            }

            public void Receive(HandlerContext context, object msg)
            {

            }

            public void Write(HandlerContext context, object msg)
            {
                context.Write(msg);
            }
        }
    }
}
