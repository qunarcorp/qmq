using System.Threading;
using System.Collections;

namespace Qunar.TC.Qmq.Client.Codec
{
    internal class Request
    {
        private static readonly Hashtable EmptyAttachments = new Hashtable();

        private static int idIndex = 0;

        private long id;

        private object args;

        private string version;

        private bool isEvent = false;

        private bool twoWay = true;

        private string target;

        public Request(object args)
        {
            id = Interlocked.Increment(ref idIndex);
            this.args = args;
        }

        public Request(long id, object args)
        {
            this.id = id;
            this.args = args;
        }

        public Request(long id)
        {
            this.id = id;
        }

        public long Id
        {
            get
            {
                return id;
            }
        }

        public object Args
        {
            get
            {
                return args;
            }

            set
            {
                this.args = value;
            }
        }

        public bool Event
        {
            get
            {
                return isEvent;
            }

            set
            {
                isEvent = value;
            }
        }

        public string Version
        {
            get { return this.version; }
            set { this.version = value; }
        }

        public bool TwoWay
        {
            get { return this.twoWay; }
            set { this.twoWay = value; }
        }

        public string Target
        {
            get { return this.target; }
            set { this.target = value; }
        }
    }
}

