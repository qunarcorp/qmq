using Microsoft.Practices.EnterpriseLibrary.Data;
using System;
using System.Data;

namespace Qunar.TC.Qmq.Client.Tx
{
    public sealed class EntlibMessageStore : MessageStore
    {
        private const string INSERT_SQL = "insert into qmq_msg_queue(message_id,content,create_time) values(@messageid,@content,@createtime)";
        private const string FINISH_SQL = "delete from qmq_msg_queue where message_id=@messageid";
        private const string ERROR_SQL = "update qmq_msg_queue set status=@status,error=error+1,update_time=@updatetime where message_id=@messageid";

        private readonly Database database;

        public EntlibMessageStore(Database db)
        {
            this.database = db;
        }

        public void Save(ProducerMessage message)
        {
            using (var command = database.GetSqlStringCommand(INSERT_SQL))
            {
                database.AddInParameter(command, "@messageid", DbType.AnsiString, message.Base.MessageId);
                database.AddInParameter(command, "@content", DbType.String, message.Base.ToString());
                database.AddInParameter(command, "@createtime", DbType.DateTime, DateTime.Now);
                database.ExecuteNonQuery(command);
            }
        }

        public void Finish(ProducerMessage message)
        {
            using (var command = database.GetSqlStringCommand(FINISH_SQL))
            {
                database.AddInParameter(command, "@messageid", DbType.AnsiString, message.Base.MessageId);
                database.ExecuteNonQuery(command);
            }
        }

        public void Error(ProducerMessage message, int status)
        {
            using (var command = database.GetSqlStringCommand(ERROR_SQL))
            {
                database.AddInParameter(command, "@status", DbType.Int16, status);
                database.AddInParameter(command, "@updatetime", DbType.DateTime, DateTime.Now);
                database.AddInParameter(command, "@messageid", DbType.AnsiString, message.Base.MessageId);
                database.ExecuteNonQuery(command);
            }
        }
    }
}
