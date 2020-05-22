using System.IO;

namespace Qunar.TC.Qmq.Client.Transport
{
    abstract class InputStream : Stream
    {
        internal abstract void MarkWriterIndex();

        internal abstract void ResetWriterIndex();

        internal abstract void MarkReaderIndex();

        internal abstract void ResetReaderIndex();

        internal abstract void Limit(int len);
    }
}
