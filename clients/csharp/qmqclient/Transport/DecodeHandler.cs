// yuzhaohui
// 2016/7/14
using System;
using System.Collections.Generic;

using Qunar.TC.Qmq.Client.Codec;
using NLog;

namespace Qunar.TC.Qmq.Client.Transport
{
    class DecodeHandler : AbstractHandler
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();

        private readonly Decoder decoder;

        private volatile Packet cumulation;

        public DecodeHandler(Decoder decoder)
        {
            this.decoder = decoder;
        }

        public override void Receive(HandlerContext context, object msg)
        {
            if (!(msg is Packet)) return;
            var p = (Packet)msg;

            bool first = cumulation == null;
            if (first)
            {
                cumulation = p;
            }
            else
            {
                ExpandCumulationOnDemand(p);
                cumulation.Write(p);
                p.Release();
            }
            Decode(context, cumulation);
        }

        private void Decode(HandlerContext context, Packet data)
        {
            List<object> output = new List<object>();
            try
            {
                var stream = new PacketInputStream(data);
                while (stream.CanRead)
                {
                    stream.MarkReaderIndex();
                    var result = decoder.Decode(stream);
                    if (result != Response.NeedMore)
                    {
                        output.Add(result);
                    }
                    else
                    {
                        stream.ResetReaderIndex();
                        return;
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error(e, "decode data error, close the channel");
                cumulation = null;
                context.Close();
            }
            finally
            {
                if (cumulation != null)
                {
                    if (!cumulation.IsReadable())
                    {
                        cumulation.Release();
                        cumulation = null;
                    }
                    else
                    {
                        cumulation.DiscardRead();
                    }
                }

                if (output.Count == 1)
                {
                    context.Receive(output[0]);
                }
                else
                {
                    for (var i = 0; i < output.Count; ++i)
                    {
                        context.Receive(output[i]);
                    }
                }
            }
        }

        private bool Enough(Packet packet)
        {
            return cumulation.Remain >= packet.Len;
        }

        private void ExpandCumulationOnDemand(Packet comming)
        {
            if (Enough(comming)) return;
            byte[] buffer = new byte[cumulation.Len + comming.Len];
            var p = new Packet(buffer, 0, 0);
            p.Write(cumulation);
            cumulation.Release();
            cumulation = p;
        }

        public override void Close(HandlerContext context)
        {
            if (cumulation != null) cumulation.Release();
            cumulation = null;
            context.Close();
        }

        public override void InActive(HandlerContext context)
        {
            if (cumulation != null) cumulation.Release();
            cumulation = null;
            context.InActive();
        }
    }
}

