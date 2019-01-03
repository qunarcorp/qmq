using System.IO;

namespace Qunar.TC.Qmq.Client.Codec.NewQmq
{
    interface PayloadHolder
    {
        void Write(Stream output);
    }
}