// yuzhaohui
// 12/30/2018
using Qunar.TC.Qmq.Client.Model;
using System;

namespace Qunar.TC.Qmq.Client.Util
{
    internal class Flags
    {
        private static long MIN_DELAY_TIME = 500;

        public static Flag GetMessageFlags(BaseMessage message)
        {
            Flag flag = Flag.Default;

            if (IsDelayMessage(message))
            {
                flag = flag | Flag.DelayMessage;
            }

            if (HasTag(message))
            {
                flag = flag | Flag.TagsMessage;
            }

            return flag;
        }

        public static bool IsDelayMessage(Flag flag)
        {
            return (flag & Flag.DelayMessage) == Flag.DelayMessage;
        }

        public static bool IsDelayMessage(BaseMessage message)
        {
            DateTime? scheduleReceiveTime = message.ScheduleReceiveTime;
            return scheduleReceiveTime.HasValue && scheduleReceiveTime.Value.Subtract(DateTime.Now).Milliseconds >= MIN_DELAY_TIME;
        }

        public static bool HasTag(Flag flag)
        {
            return (flag & Flag.TagsMessage) == Flag.TagsMessage;
        }

        public static bool HasTag(BaseMessage message)
        {
            var tags = message.GetTags();
            return tags != null && tags.Count > 0;
        }

    }
}