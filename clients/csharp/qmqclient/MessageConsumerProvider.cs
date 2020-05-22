// yuzhaohui
// 2016/7/15
using System;
using Qunar.TC.Qmq.Client.Consumer;
using Qunar.TC.Qmq.Client.Util;
using System.Threading;
using NLog;
using Qunar.TC.Qmq.Client.tag;

namespace Qunar.TC.Qmq.Client
{
    public class MessageConsumerProvider : MessageConsumer
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        //broker是用ip:port来区分consumer的，如果一个机器里部署了多个应用，端口是不能设置为一样的
        private static readonly int Port = PortSelector.Select();
        private static readonly string Host = NetUtils.LocalHost();

        private readonly MessageDistributor _distributor;

        private static int _inited = 0;

        public MessageConsumerProvider(string appCode, string metaServer)
        {
            try
            {
                if (Interlocked.CompareExchange(ref _inited, 1, 0) != 0)
                {
                    throw new Exception("MessageConsumerProvider对象很重，请确保一个应用里只有实例");
                }
                _distributor = new MessageDistributor(appCode, metaServer);
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
                throw;
            }
            finally
            {
            }
        }

        public MessageSubscriber Subscribe(string subject, string group)
        {
            if (string.IsNullOrWhiteSpace(group))
            {
                throw new ArgumentNullException(nameof(group));
            }

            var subscriber = new MessageSubscriberImpl();
            var holder = _distributor.AddListener(subject, group, subscriber.OnMessageReceived);
            subscriber.ListenerHolder = holder;
            return subscriber;
        }

    }

    internal class MessageSubscriberImpl : MessageSubscriber
    {
        private readonly object _guard = new object();
        private volatile bool _isStarted = false;

        private ConsumeMode _consumeMode = ConsumeMode.AtLeastOnce;
        private int _pullBatchSize = 50;
        private TimeSpan _pullBatchTimeout = TimeSpan.FromSeconds(1);
        private string[] _tags;
        private TagType _tagType = TagType.AND;

        public ListenerHolder ListenerHolder;

        public event MessageListener Received;

        public ConsumeMode Mode
        {
            get => _consumeMode;
            set
            {
                lock (_guard)
                {
                    if (_isStarted)
                    {
                        throw new NotSupportedException("Cannot set ConsumeMode after subscriber started");
                    }
                    _consumeMode = value;
                }
            }
        }

        public int PullBatchSize
        {
            get => _pullBatchSize;
            set
            {
                lock (_guard)
                {
                    if (_isStarted)
                    {
                        throw new NotSupportedException("Cannot set PullBatchSize after subscriber started");
                    }

                    if (_pullBatchSize <= 0)
                    {
                        throw new ArgumentException("PullBatchSize shoud be a positve value");
                    }

                    _pullBatchSize = value;
                }
            }
        }

        public TimeSpan PullBatchTimeout
        {
            get => _pullBatchTimeout;
            set
            {
                lock (_guard)
                {
                    if (_isStarted)
                    {
                        throw new NotSupportedException("Cannot set PullBatchSize after subscriber started");
                    }

                    _pullBatchTimeout = value;
                }
            }
        }

        public string[] Tags
        {
            set
            {
                lock (_guard)
                {
                    if (_isStarted)
                    {
                        throw new NotSupportedException("Cannot set Tags after subscriber started");
                    }

                    if (value == null || value.Length == 0) return;
                    _tags = new string[value.Length];
                    Array.Copy(value, _tags, value.Length);
                }
            }
        }

        public TagType TagType
        {
            set
            {
                lock (_guard)
                {
                    if (_isStarted)
                    {
                        throw new NotSupportedException("Cannot set TagType after subscriber started");
                    }

                    _tagType = value;
                }
            }
        }

        public void OnMessageReceived(Message message)
        {
            Received?.Invoke(message);
        }

        public void Start()
        {
            lock (_guard)
            {
                if (_isStarted) return;

                ListenerHolder?.Listen(new ExtraListenerConfig(_consumeMode, _pullBatchSize, _pullBatchTimeout, _tagType, _tags));
                _isStarted = true;
            }
        }

        public void Stop()
        {
            lock (_guard)
            {
                if (!_isStarted) return;

                ListenerHolder?.Stop();
                _isStarted = false;
            }
        }
    }
}
