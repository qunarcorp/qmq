namespace Qunar.TC.Qmq.Client.Util
{
    class HostAndPort
    {
        public readonly string Host;

        public readonly int Port;

        public HostAndPort(string host, int port)
        {
            Host = host;
            Port = port;
        }
    }
}
