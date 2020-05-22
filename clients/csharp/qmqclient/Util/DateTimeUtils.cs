// yuzhaohui
// 2016/8/3
using System;
namespace Qunar.TC.Qmq.Client.Util
{
    static class DateTimeUtils
    {
        private static readonly DateTime Jan1st1970 = new DateTime(1970, 1, 1, 8, 0, 0, DateTimeKind.Local);

        public static long ToTime(this DateTime time)
        {
            return (long)((time - Jan1st1970).TotalMilliseconds);
        }

        public static DateTime FromTime(long milliseconds)
        {
            return Jan1st1970.Add(TimeSpan.FromMilliseconds(milliseconds));
        }
    }
}

