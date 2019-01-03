// yuzhaohui
// 2016/7/19
using System;
namespace Qunar.TC.Qmq.Client.Model
{
    class HandlerMismatchException : Exception
    {
        private readonly string prefix;
        private readonly string group;

        public HandlerMismatchException(string prefix, string group)
        {
            this.prefix = prefix;
            this.group = group;
        }

        public string Prefix
        {
            get
            {
                return this.prefix;
            }
        }

        public string Group
        {
            get
            {
                return this.group;
            }
        }
    }
}

