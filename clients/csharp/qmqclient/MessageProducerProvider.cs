using System;
using System.Threading;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.Transport;
using Qunar.TC.Qmq.Client.Tx;
using NLog;
using Qunar.TC.Qmq.Client.Metainfo;

namespace Qunar.TC.Qmq.Client
{
    /// <summary>
    /// QMQ生产者实现接口
    ///
    /// !!!!!!!! 注意：该类较重，请确保一个应用进程内只存在少量的实例 !!!!!!
    ///
    /// DEMO
    /// 事务消息
    /// 事务消息即业务操作和消息发送在同一个事务里，满足事务的ACID特征。即业务成功消息一定会发送出去，业务失败消息一定不会发送出去
    ///
    /// var baseDao = BaseDaoFactory.CreateBaseDao("order");
    /// //这里传递的baseDao和业务使用的要相同
    /// var producer = new MessageProducerProvider(baseDao);
    ///
    /// //事务
    /// using(var scope = new TransactionScope())
    /// {
    ///     //业务操作，持久化订单
    ///     baseDao.Insert(order);
    ///
    ///     //生成一条消息，默认过期时间是15分钟
    ///     var message = producer.GenerateMessage("ctrip.hotel.order.state.changed");
    ///     message.SetProperty("orderId", order.Id);
    ///     message.SetProperty("version", 1);
    ///
    ///     //发送消息
    ///     producer.Send(message,
    ///     onSucess:(m)=>{
    ///         //记录监控
    ///     },
    ///     onFailed:(m)=>{
    ///         //记录监控
    ///     });
    /// }
    /// </summary>
    /// 
    public class MessageProducerProvider : MessageProducer, IDisposable
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        private const int DefaultExpiredTime = 15;

        private readonly string _appCode;
        private readonly string _metaServer;

        private static int _instanceCount;

        private readonly IdGenerator _idGenerator;
        private readonly QueueSender _sender;
        private readonly MessageStore _store;

        private readonly BrokerGroupService _brokerGroupService;

        public MessageProducerProvider(string appCode, string metaServer) : this(appCode, metaServer, null)
        {
        }


        public MessageProducerProvider(string appCode, string metaServer, MessageStore messageStore)
        {
            if (appCode == null || appCode.Length == 0)
            {
                throw new ArgumentException("appCode未配置");
            }

            Logger.Info("Start Constructor");
            try
            {
                if (Interlocked.Increment(ref _instanceCount) > 1)
                {
                    Logger.Warn("WARNING: MessageProducerProvider这个类很重，请确保在一个进程中创建[少量]的实例，严禁在服务的请求中创建实例。如果你确定你的用法没有问题可以忽略该错误日志");
                }

                _appCode = appCode;
                _metaServer = metaServer;
                _idGenerator = new TimestampIdGenerator();
                _brokerGroupService = new BrokerGroupService(appCode, metaServer);
                Logger.Info("idGenerator inited");
                _sender = new QueueSender(_brokerGroupService);
                _store = messageStore;
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "MessageProducerProvider Constructor error");
                throw;
            }
            finally
            {
            }
        }

        public Message GenerateMessage(string subject)
        {
            subject = Validate(subject);
            var message = new BaseMessage(subject, _idGenerator.Generate());
            message.SetExpiredTime(TimeSpan.FromMinutes(15));
            message.SetProperty(BaseMessage.keys.qmq_appCode, _appCode);
            return message;
        }

        private string Validate(string subject)
        {
            if (subject == null)
            {
                throw new ArgumentNullException("subject");
            }
            subject = subject.Trim();
            if (subject.Length == 0)
            {
                throw new ArgumentException("subject is empty");
            }
            return subject;
        }

        public void Send(Message message, Action<Message> onSuccess = null, Action<Message> onFailed = null)
        {
            try
            {
                var producerMessage = new ProducerMessageImpl((BaseMessage)message, new Listener(onSuccess, onFailed), _sender);
                if (TransactionMessageHolder.InsertMessage(producerMessage, _store)) return;

                producerMessage.Send();
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
                throw new Exception("Send message error", ex);
            }
            finally
            {
            }
        }

        private void InternalDispose()
        {
            _sender?.Dispose();
        }

        public void Dispose()
        {
            InternalDispose();
            GC.SuppressFinalize(this);
        }

        ~MessageProducerProvider()
        {
            InternalDispose();
        }
    }

    internal class Listener : MessageSendStateListener
    {
        private readonly Action<Message> _onSuccess;
        private readonly Action<Message> _onFailed;

        public Listener(Action<Message> onSuccess, Action<Message> onFailed)
        {
            _onSuccess = onSuccess;
            _onFailed = onFailed;
        }

        public void OnSuccess(Message message)
        {
            _onSuccess?.Invoke(message);
        }

        public void OnFailed(Message message)
        {
            _onFailed?.Invoke(message);
        }
    }
}
