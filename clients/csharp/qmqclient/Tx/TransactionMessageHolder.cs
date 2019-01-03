using System;
using System.Threading;
using System.Transactions;
using System.Collections.Generic;
using NLog;

namespace Qunar.TC.Qmq.Client.Tx
{
    internal class TransactionMessageHolder
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        private static readonly ThreadLocal<Dictionary<Transaction, TransactionMessageHolder>> Holder = new ThreadLocal<Dictionary<Transaction, TransactionMessageHolder>>();

        public static bool InsertMessage(ProducerMessageImpl message, MessageStore messageStore)
        {
            try
            {
                if (!message.Base.Durable) return false;

                //默认 Durable 为true, 消息持久化，Store 对象为null 时
                if (messageStore == null)
                {
                    message.Base.Durable = false;
                    return false;
                }
                message.MessageStore = messageStore;

                var transaction = Transaction.Current;
                if (transaction == null)
                {
                    try
                    {
                        message.Save();
                    }
                    catch (Exception e)
                    {
                        message.Base.StoreAtFailed = true;
                    }
                    return false;
                }

                var map = Holder.Value;
                if (map == null)
                {
                    map = new Dictionary<Transaction, TransactionMessageHolder>();
                    Holder.Value = map;
                }
                map.TryGetValue(transaction, out var container);
                if (container == null)
                {
                    container = new TransactionMessageHolder();
                    map[transaction] = container;
                    transaction.TransactionCompleted += container.OnTransactionCompleted;
                }
                container.Add(message);
                return true;
            }
            catch (Exception ex)
            {

                Logger.Error(ex, "TransactionMessageHolder InsertMessage error");
                throw;
            }
            finally
            {
            }
        }

        private void OnTransactionCompleted(object sender, TransactionEventArgs e)
        {
            var transaction = e.Transaction;
            try
            {
                var status = transaction.TransactionInformation.Status;
                if (status == TransactionStatus.Committed)
                {
                    SendMessages();
                }
                else if (status == TransactionStatus.Aborted)
                {
                    Warning();
                }
                else
                {
                    WarningUnkonwn();
                }
            }
            finally
            {
                Holder.Value.Remove(transaction);
                transaction.TransactionCompleted -= this.OnTransactionCompleted;
            }

        }

        void WarningUnkonwn()
        {
            Logger.Warn("事务状态未知，等待自动补偿");
        }

        private void SendMessages()
        {
            if (_queue == null) return;
            foreach (var message in _queue)
            {
                message.Send();
            }
            _queue.Clear();
            _queue = null;
        }

        private void Warning()
        {
            Logger.Warn("事务回滚，消息将不会被发送");
        }

        private IList<ProducerMessageImpl> _queue;

        private void Add(ProducerMessageImpl message)
        {
            if (_queue == null)
                _queue = new List<ProducerMessageImpl>();

            _queue.Add(message);
            message.Save();
        }
    }
}