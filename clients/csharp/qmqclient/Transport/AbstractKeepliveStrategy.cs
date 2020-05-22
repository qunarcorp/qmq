using Qunar.TC.Qmq.Client.Util;
using System;

namespace Qunar.TC.Qmq.Client.Transport
{
    internal abstract class AbstractKeepliveStrategy : IKeepliveStrategy
    {
        private readonly long _pingInterval;
        private readonly long _sessionTimeout;

        private long _lastCheck;

        protected AbstractKeepliveStrategy(long pingInterval, long sessionTimeout)
        {
            _pingInterval = pingInterval;
            _sessionTimeout = sessionTimeout;
        }

        public void CheckStatus(TransportClient client, long lastWrite, long lastRead, long lastConnect)
        {
            if (client.IsClose) return;
            long now = DateTime.Now.ToTime();
            if (now - _lastCheck < _pingInterval) return;
            _lastCheck = now;

            if (!client.IsConnected()) return;
            if (IsSessionTimeout(now, lastRead, lastConnect))
            {
                SessionTimeout(client);
                return;
            }
            if (IsWriteIdleTimeout(now, lastWrite) || IsReadIdleTimeout(now, lastRead))
            {
                Ping(client);
            }
        }

        private bool IsSessionTimeout(long now, long lastRead, long lastConnect)
        {
            return now - lastRead >= _sessionTimeout && now - lastConnect >= _pingInterval;
        }

        private bool IsWriteIdleTimeout(long now, long lastWrite)
        {
            return now - lastWrite >= _pingInterval;
        }

        private bool IsReadIdleTimeout(long now, long lastRead)
        {
            return now - lastRead >= _pingInterval;
        }

        protected virtual void SessionTimeout(TransportClient client)
        {
            client.CloseChannel();
        }

        protected abstract void Ping(TransportClient client);
    }
}
