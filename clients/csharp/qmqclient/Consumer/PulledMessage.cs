using System;
using System.Threading;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.NewQmq;
using Qunar.TC.Qmq.Client.Util;

namespace Qunar.TC.Qmq.Client.Consumer
{
    internal class PulledMessage : BaseMessage
    {
        private const int UnAcked = 0;
        private const int Acked = 1;

        private readonly AckHandler _handler;

        private volatile bool _autoAck = true;
        private int _state = UnAcked;

        private long _pulledTime;
        private long _startConsume;

        public PulledMessage(BaseMessage message, AckHandler handler)
            : base(message)
        {
            _handler = handler ?? throw new ArgumentNullException(nameof(handler));
            _pulledTime = DateTime.Now.ToTime();
        }

        public override void Ack(long elapsed, Exception e)
        {
            if (Interlocked.CompareExchange(ref _state, Acked, UnAcked) == UnAcked)
            {
                _handler.Ack(elapsed, e);
            }
        }

        public override bool AutoAck
        {
            get => _autoAck;
            set => _autoAck = value;
        }

        public override int Times
        {
            get
            {
                var name = Enum.GetName(typeof(keys), keys.qmq_times);
                var ret = GetIntProperty(name);
                return ret ?? 0;
            }
        }

        public void SetStart(long start)
        {
            _startConsume = start;
        }

        public long PulledTime
        {
            get
            {
                return _pulledTime;
            }
        }
    }
}