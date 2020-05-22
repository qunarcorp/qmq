// yuzhaohui
// 2016/7/19
namespace Qunar.TC.Qmq.Client.Model
{
    internal class ChannelState
    {
        private readonly long currentTime;
        private readonly int activeCount;

        public ChannelState(int activeCount, long currentTime)
        {
            this.currentTime = currentTime;
            this.activeCount = activeCount;
        }

        public long CurrentTime => currentTime;

        public int ActiveCount => activeCount;
    }
}

