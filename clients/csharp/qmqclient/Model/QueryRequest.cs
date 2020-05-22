// yuzhaohui
// 2016/7/19
namespace Qunar.TC.Qmq.Client.Model
{
    internal class QueryRequest
    {
        private string prefix;
        private string group;
        private string messageId;

        public QueryRequest()
        {
        }

        public string Prefix
        {
            get => prefix;
            set => prefix = value;
        }

        public string Group
        {
            get => group;
            set => group = value;
        }

        public string MessageId
        {
            get => messageId;
            set => messageId = value;
        }


    }
}

