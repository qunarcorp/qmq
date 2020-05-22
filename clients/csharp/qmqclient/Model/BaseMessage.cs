using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

using Qunar.TC.Qmq.Client.Util;
using System.Web;

namespace Qunar.TC.Qmq.Client.Model
{
    internal class BaseMessage : Message
    {
        private string subject;

        private string messageId;

        private bool durable = true;

        private Hashtable attrs = new Hashtable();

        private string[] tags;

        private static readonly ISet<string> keyNames = new HashSet<string>();

        static BaseMessage()
        {
            string[] arr = Enum.GetNames(typeof(keys));
            for (int i = 0; i < arr.Length; ++i)
            {
                keyNames.Add(arr[i]);
            }
        }

        public enum keys
        {
            qmq_createTime,
            qmq_expireTime,
            qmq_consumerGroupName,
            qmq_brokerGroupName,
            qmq_scheduleRecevieTime,
            qmq_times,
            qmq_appCode,
            qmq_pullOffset,
            qmq_consumerOffset,
            qmq_corruptData
        }

        public BaseMessage() { }

        public BaseMessage(string subject, string messageId)
        {
            this.subject = subject;
            this.messageId = messageId;
            SetProperty(keys.qmq_createTime, DateTime.Now.ToTime());
        }

        public BaseMessage(BaseMessage message)
            : this(message.subject, message.messageId)
        {
            this.attrs = new Hashtable(message.attrs);
        }

        internal BaseMessage(string subject, BaseMessage message)
            : this(subject, message.messageId)
        {
            attrs = new Hashtable(message.attrs);
        }

        public string Subject
        {
            get
            {
                return this.subject;
            }
        }

        public string MessageId
        {
            get
            {
                return this.messageId;
            }
        }

        public DateTime? CreatedTime
        {
            get
            {
                return GetDateProperty(keys.qmq_createTime.ToString());
            }
        }

        public DateTime? ExpiredTime
        {
            get
            {
                return GetDateProperty(keys.qmq_expireTime.ToString());
            }
        }

        public DateTime? ScheduleReceiveTime
        {
            get
            {
                return GetDateProperty(keys.qmq_scheduleRecevieTime.ToString());
            }
        }

        public void SetProperty<T>(keys key, T value)
        {
            attrs[Enum.GetName(typeof(keys), key)] = value;
        }

        public void SetProperty(string name, bool value)
        {
            SetObjectProperty(name, value);
        }

        public void SetProperty(string name, int value)
        {
            SetObjectProperty(name, value);
        }

        public void SetProperty(string name, long value)
        {
            SetObjectProperty(name, value);
        }

        public void SetProperty(string name, float value)
        {
            SetObjectProperty(name, value);
        }

        public void SetProperty(string name, double value)
        {
            SetObjectProperty(name, value);
        }

        public void SetProperty(string name, DateTime date)
        {
            SetObjectProperty(name, date.ToTime());
        }

        public void SetProperty(string name, string value)
        {
            SetObjectProperty(name, value);
        }

        public void SetLargeString(string name, string value)
        {
            LargeStringUtil.SetLargeString(this, name, value);
        }

        public void RemoveProperty(keys key)
        {
            attrs.Remove(key.ToString());
        }

        public string GetLargeString(string name)
        {
            return LargeStringUtil.GetLargeString(this, name);
        }

        public string GetStringProperty(string name)
        {
            var v = attrs[name];

            return v?.ToString();
        }

        public string GetStringProperty(keys key)
        {
            var name = Enum.GetName(typeof(keys), key);
            return GetStringProperty(name);
        }

        public bool? GetBooleanProperty(string name)
        {
            object v = attrs[name];
            if (v == null)
                return null;

            var r = v as bool?;
            if (r != null) return r.Value;
            return bool.Parse(v.ToString());
        }

        public DateTime? GetDateProperty(string name)
        {
            var o = attrs[name];
            switch (o)
            {
                case long lv:
                    return DateTimeUtils.FromTime(lv);
                case string s:
                    return DateTimeUtils.FromTime(long.Parse(s));
                default:
                    return null;
            }
        }

        public int? GetIntProperty(string name)
        {
            object v = attrs[name];
            if (v == null)
                return null;

            var r = v as int?;
            if (r != null) return r.Value;
            return int.Parse(v.ToString());
        }

        public long? GetLongProperty(string name)
        {
            object v = attrs[name];
            if (v == null)
                return null;

            var r = v as long?;
            if (r != null) return r.Value;
            return long.Parse(v.ToString());
        }

        public float? GetFloatProperty(string name)
        {
            object v = attrs[name];
            if (v == null)
                return null;

            var r = v as float?;
            if (r != null) return r.Value;
            return FloatingParser.ParseFloat(v.ToString());
        }

        public double? GetDoubleProperty(string name)
        {
            object v = attrs[name];
            if (v == null)
                return null;

            var r = v as double?;
            if (r != null) return r.Value;
            return FloatingParser.ParseDouble(v.ToString());
        }

        public void SetDelayTime(DateTime date)
        {
            var expiredTime = (long)attrs[keys.qmq_expireTime.ToString()];
            var createdTime = (long)attrs[keys.qmq_createTime.ToString()];
            var deadline = date.ToTime();
            attrs[keys.qmq_expireTime.ToString()] = deadline + (expiredTime - createdTime);
            attrs[keys.qmq_scheduleRecevieTime.ToString()] = deadline;
        }

        public void SetDelayTime(TimeSpan span)
        {
            SetDelayTime(DateTime.Now.Add(span));
        }

        public void SetExpiredTime(TimeSpan span)
        {
            var expiredTime = DateTime.Now.Add(span);
            SetExpiredTime(expiredTime);
        }

        public void SetExpiredTime(DateTime expiredTime)
        {
            SetProperty(keys.qmq_expireTime, expiredTime.ToTime());
        }


        internal void SetPropertyForInternal(String name, object value)
        {
            attrs[name] = value;
        }

        private void SetObjectProperty(String name, object value)
        {
            if (keyNames.Contains(name))
                throw new InvalidOperationException("property name [" + name + "] is protected. ");
            attrs[name] = value;
        }

        public virtual int Times
        {
            get
            {
                throw new NotSupportedException("该方法只有在consumer端生效");
            }
        }

        public virtual bool AutoAck
        {
            get
            {
                throw new NotSupportedException("该方法只有在consumer端生效");
            }
            set
            {
                throw new NotSupportedException("只在consumer端可以设置");
            }
        }

        public bool StoreAtFailed
        {
            set;
            get;
        }

        public void Ack(long elapsed)
        {
            Ack(elapsed, null);
        }

        public virtual void Ack(long elapsed, Exception e)
        {
            throw new NotSupportedException("该方法只在consumer端生效");
        }

        public bool Durable
        {
            get { return this.durable; }
            set { this.durable = value; }
        }

        public Hashtable Attrs
        {
            get
            {
                return attrs;
            }
        }

        public void SetTags(string[] tags)
        {
            this.tags = tags;
        }

        public IList<string> GetTags()
        {
            if (tags == null) return null;
            return new List<string>(tags);
        }

        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append("{\"subject\":").Append("\"").Append(subject).Append("\",");
            result.Append("\"messageId\":").Append("\"").Append(messageId).Append("\",");
            result.Append("\"durable\":").Append(durable.ToString().ToLower()).Append(',');
            AppendTags(result);
            AppendAttrs(result);
            result.Append('}');
            return result.ToString();
        }

        void AppendTags(StringBuilder result)
        {
            if (tags == null || tags.Length == 0) return;
            result.Append("\"tags\":[");
            for (int i = 0; i < tags.Length; ++i)
            {
                result.Append("\"");
                result.Append(tags[i]);
                result.Append("\"");
                if (i < tags.Length - 1)
                {
                    result.Append(',');
                }
            }
            result.Append(']');
        }

        private void AppendAttrs(StringBuilder result)
        {
            var count = 0;
            result.Append("\"attrs\":{");
            foreach (DictionaryEntry item in attrs)
            {
                result.Append("\"").Append(item.Key).Append("\":");
                AppendValue(result, item.Value);
                if (++count < attrs.Count)
                {
                    result.Append(',');
                }
            }
            result.Append('}');
        }

        private void AppendValue(StringBuilder result, object value)
        {
            if (value == null)
            {
                result.Append("null");
                return;
            }

            if (value is string)
            {
                result.Append("\"").Append(HttpUtility.JavaScriptStringEncode((string)value)).Append("\"");
                return;
            }

            if (value is bool)
            {
                result.Append(value.ToString().ToLower());
                return;
            }

            if (value is DateTime)
            {
                result.Append(DateTimeUtils.ToTime((DateTime)value));
                return;
            }

            if (value.GetType().IsPrimitive)
            {
                result.Append(value);
            }
        }
    }
}

