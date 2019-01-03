using System.IO;

namespace Qunar.TC.Qmq.Client.Codec
{
    interface Encoder
    {
        void Encode(object msg, Stream stream);
    }
}

