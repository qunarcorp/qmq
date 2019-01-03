using System;
using System.Threading;

namespace Qunar.TC.Qmq.Client.Util
{
    internal static class StaticRandom
    {
        private static int _seed = Environment.TickCount;
        private static readonly ThreadLocal<Random> LocalRandom =
            new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref _seed)));

        public static int NextRand(int max)
        {
            return LocalRandom.Value.Next(max);
        }

        public static int NextRand(int min, int max)
        {
            return LocalRandom.Value.Next(min, max);
        }
    }
}