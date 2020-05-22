using System;

namespace Qunar.TC.Qmq.Client.Exceptions
{
    internal class NoWritableBrokerException : Exception
    {
        internal NoWritableBrokerException(string message)
            : base(message)
        {
        }
    }
}