// yuzhaohui
// 12/5/2016

using System.Net;
using System.Net.Sockets;
namespace Qunar.TC.Qmq.Client.Transport
{
    interface Channel
    {
        void UpdateSocket(Socket socket);

        void Write(object msg);

        void Receive();

        bool Writtable { get; }

        bool Closed { get; }

        void Close();

        void Release();

        ChannelPipeline Pipeline();

        EndPoint RemoteEndPoint();
    }
}
