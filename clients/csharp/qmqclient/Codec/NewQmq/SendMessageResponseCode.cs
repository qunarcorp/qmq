namespace Qunar.TC.Qmq.Client.Codec.NewQmq
{

    /// <summary>
    /// broker处理请求返回的状态码
    /// </summary>
    class SendMessageResponseCode
    {
        //成功
        public const short SUCCESS = 0;
        //繁忙
        public const short BUSY = 1;
        //重复投递的消息
        public const short DUPLICATE = 2;
        //消息主题未注册
        public const short SUBJECT_NOT_ASSIGNED = 3;
        // 消息投递到slave broker了
        public const short BROKER_READ_ONLY = 4;
        //被broker拦截
        public const short BROKER_REJECT = 5;
        //存储失败，但broker成功接收
        public const short BROKER_SAVE_FAILED = 6;

    }
}
