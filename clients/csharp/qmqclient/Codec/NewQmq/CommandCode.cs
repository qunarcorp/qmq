namespace Qunar.TC.Qmq.Client.Codec.NewQmq
{
    /// <summary>
    ///     对应header code
    /// </summary>
    internal static class CommandCode
    {
        /// <summary>
        ///     响应码（response code）
        /// </summary>
        public const short Placeholder = -1;

        public const short Success = 0;
        public const short UnknownCode = 3;
        public const short NoMessage = 4;
        public const short BrokerError = 51;
        public const short BrokerReject = 52;


        /// <summary>
        ///     请求码（request code）
        /// </summary>
        public const short SendMessage = 10;
        public const short PullMessage = 11;
        public const short AckRequest = 12;
        public const short SyncRequest = 20;
        public const short QueueCount = 26;

        public const short BrokerRegister = 30;
        public const short ClientRegister = 35;

        public const short BrokerAcquireMeta = 40;

        public const short UidAssign = 50;
        public const short UidAcquire = 51;

        public const short Heartbeat = 100;
    }
}