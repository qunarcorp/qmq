using System.Threading;

namespace Qunar.TC.Qmq.Client.Transport
{
    internal static class ChannelContext
    {
        private static readonly ThreadLocal<string> Remote = new ThreadLocal<string>();

        public static string RemoteAddress
        {
            set => Remote.Value = value;

            get => Remote.Value;
        }
    }
}
