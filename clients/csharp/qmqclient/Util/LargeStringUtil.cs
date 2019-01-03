using System;
using System.Text;

namespace Qunar.TC.Qmq.Client.Util
{
    internal static class LargeStringUtil
    {
        private const short _32K = (32 * 1024) / 4;

        public static void SetLargeString(Message msg, string key, string data)
        {
            var partIdx = 0;
            for (int remain = data.Length; remain > 0; remain -= _32K)
            {
                int startIdx = partIdx * _32K;
                string part = data.Substring(startIdx, Math.Min(_32K, remain));
                msg.SetProperty(BuildPartKey(key, partIdx), part);
                partIdx += 1;
            }
        }

        public static string GetLargeString(Message msg, string key)
        {
            var small = msg.GetStringProperty(key);
            if (small != null) return small;

            var result = new StringBuilder();
            var partIdx = 0;
            while (true)
            {
                string part = msg.GetStringProperty(BuildPartKey(key, partIdx));
                if (part == null)
                {
                    break;
                }

                partIdx += 1;
                result.Append(part);
            }
            return result.ToString();
        }

        private static string BuildPartKey(string key, int idx)
        {
            return $"{key}#part{idx}";
        }
    }
}
