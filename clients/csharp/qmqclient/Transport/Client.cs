using Qunar.TC.Qmq.Client.Codec;

namespace Qunar.TC.Qmq.Client.Transport
{
	interface Client
	{
		void Send(object request);
	}
}

