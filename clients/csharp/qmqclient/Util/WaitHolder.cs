using System;
using System.Threading;

namespace Qunar.TC.Qmq.Client.Util
{
    internal class WaitHolder<T>
    {
        private readonly ManualResetEventSlim _waitAvaliable = new ManualResetEventSlim();
        private readonly object _updateGuard = new object();
        private T _result;

        public bool AlreadySet => _waitAvaliable.IsSet;

        public T Result
        {
            get
            {
                _waitAvaliable.Wait();
                lock (_updateGuard)
                {
                    return _result;
                }
            }
            set
            {
                lock (_updateGuard)
                {
                    _result = value;
                }
                _waitAvaliable.Set();
            }
        }
    }
}
