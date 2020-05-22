using System;
using System.Collections.Generic;
using NLog;

namespace Qunar.TC.Qmq.Client.Util
{
    internal static class DictValueFilter
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        public static List<TValue> Filter<TKey, TValue>(Dictionary<TKey, TValue> dict, Func<TValue, bool> pred)
        {
            var result = new List<TValue>();

            var source = dict.Values;
            var enumerator = source.GetEnumerator();
            try
            {
                while (enumerator.MoveNext())
                {
                    var value = enumerator.Current;
                    if (pred(value))
                    {
                        result.Add(value);
                    }
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, $"ValueCollection filter failed. count: {source.Count}, values: {string.Join(", ", source)}");
                throw;
            }
            finally
            {
                enumerator.Dispose();
            }

            return result;
        }
    }
}