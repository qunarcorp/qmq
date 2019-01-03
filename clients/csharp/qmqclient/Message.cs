using System;
using System.Collections.Generic;

namespace Qunar.TC.Qmq.Client
{

    public interface Message
    {
        string MessageId
        {
            get;
        }

        string Subject
        {
            get;
        }

        DateTime? CreatedTime
        {
            get;
        }

        DateTime? ExpiredTime
        {
            get;
        }

        DateTime? ScheduleReceiveTime
        {
            get;
        }

        bool AutoAck
        {
            set;
        }

        bool StoreAtFailed
        {
            set;
            get;
        }

        /**
        * 第几次发送
        * 使用方应该监控该次数，如果不是刻意设计该次数不应该太多
        *
        * @return
        */
        int Times
        {
            get;
        }

        void SetProperty(string name, bool value);

        void SetProperty(string name, int value);

        void SetProperty(string name, long value);

        void SetProperty(string name, float value);

        void SetProperty(string name, double value);

        void SetProperty(string name, DateTime date);

        void SetProperty(string name, string value);

        void SetLargeString(string name, string value);

        string GetLargeString(string name);

        bool? GetBooleanProperty(string name);

        int? GetIntProperty(string name);

        long? GetLongProperty(string name);

        float? GetFloatProperty(string name);

        double? GetDoubleProperty(string name);

        DateTime? GetDateProperty(string name);

        string GetStringProperty(string name);

        void SetDelayTime(DateTime date);

        void SetDelayTime(TimeSpan span);

        void Ack(long elapsed);

        void Ack(long elapsed, Exception e);

        bool Durable { get; set; }

        void SetTags(string[] tags);

        IList<string> GetTags();
    }
}

