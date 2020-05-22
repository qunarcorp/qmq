using Qunar.TC.Qmq.Client.Transport;

namespace Qunar.TC.Qmq.Client.Codec
{
    interface Decoder
    {
        object Decode(InputStream stream);
    }
}

