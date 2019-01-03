// yuzhaohui
// 2016/7/14
using System.IO;

using Qunar.TC.Qmq.Client.Codec;

namespace Qunar.TC.Qmq.Client.Transport
{
    class EncodeHandler : AbstractHandler
    {
        private readonly Encoder encoder;

        public EncodeHandler(Encoder encoder)
        {
            this.encoder = encoder;
        }

        public override void Write(HandlerContext context, object msg)
        {
            var stream = new MemoryStream();
            encoder.Encode(msg, stream);
            var buffer = stream.ToArray();
            context.Write(buffer);
        }
    }
}

