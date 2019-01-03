using System.Collections.Generic;

namespace Qunar.TC.Qmq.Client.Util
{
    internal static class FloatingParser
    {
        public const string JavaPositiveInfinity = "Infinity";
        public const string JavaNegativeInfinity = "-Infinity";

        private static readonly HashSet<string> Infinity = new HashSet<string> { JavaPositiveInfinity, JavaNegativeInfinity };

        public static float ParseFloat(string s)
        {
            if (!Infinity.Contains(s))
            {
                return float.Parse(s);
            }

            return JavaPositiveInfinity.Equals(s) ? float.PositiveInfinity : float.NegativeInfinity;
        }

        public static double ParseDouble(string s)
        {
            if (!Infinity.Contains(s))
            {
                return double.Parse(s);
            }

            return JavaPositiveInfinity.Equals(s) ? double.PositiveInfinity : double.NegativeInfinity;
        }
    }
}