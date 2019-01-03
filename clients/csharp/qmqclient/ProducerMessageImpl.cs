// yuzhaohui
// 11/29/2016
using Qunar.TC.Qmq.Client.Exceptions;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.Transport;
using Qunar.TC.Qmq.Client.Tx;
using NLog;
using System;
using System.Threading;

namespace Qunar.TC.Qmq.Client
{
    internal class ProducerMessageImpl : ProducerMessage
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        private const int MaxRetries = 3;

        private readonly BaseMessage _message;
        private readonly MessageSendStateListener _listener;
        private readonly Sender _sender;
        private MessageStore _messageStore;

        private volatile object _routeKey;

        private int _retries = 0;

        public ProducerMessageImpl(BaseMessage message, MessageSendStateListener listener, Sender sender)
        {
            _message = message;
            _listener = listener;
            _sender = sender;
        }

        public Message Base => _message;

        public void Save()
        {
            if (_message.StoreAtFailed) return;
            _messageStore?.Save(this);
        }

        public void Send()
        {
            _sender.Send(this);
        }

        public void Success()
        {
            try
            {
                if (_messageStore != null && !_message.StoreAtFailed)
                {
                    _messageStore.Finish(this);
                }

                OnSuccess();
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
            }
            finally
            {
            }
        }

        public void Failed(Exception e)
        {
            try
            {
                if (CanRetry(e))
                {
                    _sender.Send(this);
                    return;
                }

                Logger.Info(e, "Send message failed");

                if (_messageStore != null)
                {
                    if (_message.StoreAtFailed)
                    {
                        _messageStore.Save(this);
                    }
                    else
                    {
                        _messageStore.Error(this, -100);
                    }
                }

                OnFailed();
            }
            catch (Exception ex)
            {
                Logger.Error(ex);
            }
            finally
            {
            }
        }

        private bool CanRetry(Exception e)
        {
            return NeedRetryException(e) && Interlocked.Increment(ref _retries) <= MaxRetries;
        }

        private static bool NeedRetryException(Exception e)
        {
            return e is TimeoutException || e is NoWritableBrokerException || e is BrokerReadOnlyException;
        }

        public object RouteKey
        {
            get => _routeKey;
            set => _routeKey = value;
        }

        public MessageStore MessageStore
        {
            get => _messageStore;
            set => _messageStore = value;
        }

        private void OnSuccess()
        {
            try
            {
                _listener?.OnSuccess(_message);
            }
            catch
            {

            }
        }

        private void OnFailed()
        {
            try
            {
                _listener?.OnFailed(_message);
            }
            catch
            {

            }
        }
    }
}
