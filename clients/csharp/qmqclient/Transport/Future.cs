// yuzhaohui
// 12/3/2016
using System;
using System.Threading;

namespace Qunar.TC.Qmq.Client.Transport
{
    internal class Future<T> where T : class
    {
        private readonly ManualResetEventSlim _event;
        public readonly DateTime sent;
        public readonly int timeout;

        private T result;
        private object source;

        public Future(int timeout)
        {
            this.sent = DateTime.Now;
            this.timeout = timeout;
            this._event = new ManualResetEventSlim();
        }

        public T Result
        {
            get
            {
                this._event.Wait();
                return this.result;
            }

            set
            {
                this.result = value;
                this._event.Set();
            }
        }

        public object Source
        {
            get
            {
                return source;
            }
            set
            {
                source = value;
            }
        }

        public bool IsDone()
        {
            return result != null;
        }
    }
}
