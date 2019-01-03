// yuzhaohui
// 2016/7/15

using System;
using System.Collections;
using System.Collections.Concurrent;
using Qunar.TC.Qmq.Client.Dubbo;
using Qunar.TC.Qmq.Client.Model;

namespace Qunar.TC.Qmq.Client.Consumer
{
    internal class MessageDistributor : IRequestHandler
    {
        private static readonly NLog.Logger Logger = NLog.LogManager.GetCurrentClassLogger();

        private readonly IConsumerRegister _register;
        private readonly ConcurrentDictionary<string, MessageHandler> _handlers;

        private readonly PulledMessageHandler _pulledMessageHandler;

        public MessageDistributor(string appCode, string metaServer)
        {
            _register = new PullConsumerRegister(appCode, metaServer, this);
            _handlers = new ConcurrentDictionary<string, MessageHandler>();
            _pulledMessageHandler = new PulledMessageHandler();
        }

        public ListenerHolder AddListener(string prefix, string group, MessageListener listener)
        {
            try
            {
                var key = KeyOf(prefix, group);
                var handler = new MessageHandler(listener);
                if (!_handlers.TryAdd(key, handler))
                {
                    throw new InvalidOperationException($"重复注册listener: prefix-> {prefix}, group-> {group}");
                }
                Logger.Info($"订阅subject prefix为 {prefix} 的消息， consumer group为 {group}");

                return new ListenerHolderImpl(_register, prefix, group);
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

        object IRequestHandler.Handle(object msg)
        {
            return TaskReceived(_pulledMessageHandler.CreateTask((PulledMessage)msg));
        }

        private object TaskReceived(IMessageHandleTask task)
        {
            var message = task.Message();
            try
            {
                return DispatchTask(task);
            }
            catch (Exception ex)
            {
                throw;
            }
            finally
            {
            }
        }

        private object DispatchTask(IMessageHandleTask task)
        {
            var message = task.Message();
            var subject = message.Subject;
            var group = message.GetStringProperty(BaseMessage.keys.qmq_consumerGroupName);
            var key = KeyOf(subject, group);

            _handlers.TryGetValue(key, out var handler);
            if (handler == null)
            {
                return new ConsumerRejectException("消费者尚未初始化完成");
            }
            return task.DispatchTo(handler);
        }

        private static string KeyOf(string prefix, string group)
        {
            return $"{prefix}/{group}";
        }
    }

    internal class ListenerHolderImpl : ListenerHolder
    {
        private readonly IConsumerRegister _register;
        private readonly string _prefix;
        private readonly string _group;

        public ListenerHolderImpl(IConsumerRegister register, string prefix, string group)
        {
            _register = register;
            _prefix = prefix;
            _group = group;
        }

        public void Listen(ExtraListenerConfig config)
        {
            _register.Registe(_prefix, _group, config);
        }

        public void Stop()
        {
            _register.Unregiste(_prefix, _group);
        }
    }
}
