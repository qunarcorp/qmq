// yuzhaohui
// 12/30/2018
using System;
namespace Qunar.TC.Qmq.Client
{
    [Flags]
    internal enum Flag
    {
        Default = 0,
        DelayMessage = 2,
        TagsMessage = 4
    }
}
