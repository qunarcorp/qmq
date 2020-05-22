using System.IO;

namespace Qunar.TC.Qmq.Client.Codec.NewQmq
{
    class Datagram
    {
        private PayloadHolder payloadHolder;

        public RemotingHeader Header { get; set; }

        public byte[] Body { get; set; }

        public PayloadHolder PayloadHolder
        {
            set
            {
                this.payloadHolder = value;
            }
        }

        public void WriteBody(Stream output)
        {
            payloadHolder?.Write(output);
        }
    }
}
