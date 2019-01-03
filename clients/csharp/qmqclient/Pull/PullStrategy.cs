using Qunar.TC.Qmq.Client.Util;
using System;

namespace Qunar.TC.Qmq.Client.Pull
{
    interface PullStrategy
    {
        bool NeedPull();

        void Record(bool status);
    }

    class WeightPullStrategy : PullStrategy
    {
        private const int MinWeight = 1;
        private const int MaxWeight = 32;

        private int _weight = MaxWeight;

        public bool NeedPull()
        {
            return RandomWeightThreshold() < _weight;
        }

        private int RandomWeightThreshold()
        {
            return StaticRandom.NextRand(0, MaxWeight);
        }

        public void Record(bool status)
        {
            if (status)
            {
                var weight = _weight * 2;
                _weight = Math.Min(weight, MaxWeight);
            }
            else
            {
                var weight = _weight / 2;
                _weight = Math.Max(weight, MinWeight);
            }
        }
    }

    class AlwaysPullStrategy : PullStrategy
    {
        public bool NeedPull()
        {
            return true;
        }

        public void Record(bool status)
        {

        }
    }
}
