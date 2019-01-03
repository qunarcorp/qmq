namespace Qunar.TC.Qmq.Client.Codec.NewQmq
{
    internal class RemotingHeader
    {
        public const int QmqMagicCode = unchecked((int)0xdec10ade);

        public const short Version3 = 3;
        public const short Version4 = 4;
        public const short Version8 = 8;

        public const short MinHeaderSize = 16;
        public const short HeaderSizeLen = 2;
        public const short TotalSizeLen = 4;
        public const short LengthFieldLen = TotalSizeLen + HeaderSizeLen;

        public short Code { get; set; }

        public short Version { get; set; }

        public int Opaque { get; set; }

        public int Flag { get; set; }

        public short RequestCode { get; set; }

        public int MagicCode { get; set; }
    }
}