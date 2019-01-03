using Qunar.TC.Qmq.Client.Codec;
using Qunar.TC.Qmq.Client.Codec.NewQmq;
using Qunar.TC.Qmq.Client.Model;
using Qunar.TC.Qmq.Client.Util;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Qunar.TC.Qmq.Client.NewQmq
{
    class MessageSendPayloadHolder : PayloadHolder
    {
        private List<BaseMessage> messages;

        public MessageSendPayloadHolder(List<BaseMessage> messages)
        {
            this.messages = messages;
        }

        public MessageSendPayloadHolder(List<Message> messages)
        {
            this.messages = messages.ConvertAll(ConvertTo);
        }

        public void Write(Stream output)
        {
            if (messages == null || !messages.Any()) return;

            foreach (var message in messages)
            {
                SerializeMessage(message, output);
            }

        }

        private void SerializeMessage(BaseMessage message, Stream output)
        {
            var crcIndex = (int)output.Position;
            output.Position = crcIndex + 8; //这里预留8保存crc信息

            var messageStart = (int)output.Position;

            Flag flag = Flags.GetMessageFlags(message);
            // flag
            ByteBufHelper.WriteByte((byte)flag, output);

            byte[] buffer = new byte[8];
            // created time
            ByteBufHelper.WriteInt64(message.CreatedTime.Value.ToTime(), buffer, output);

            if (Flags.IsDelayMessage(flag))
            {
                //Schedule Time
                ByteBufHelper.WriteInt64(message.ScheduleReceiveTime.Value.ToTime(), buffer, output);
            }
            else
            {
                // expired time
                ByteBufHelper.WriteInt64(message.ExpiredTime.Value.ToTime(), buffer, output);
            }


            // subject
            ByteBufHelper.WriteString(message.Subject, buffer, output);

            // message id
            ByteBufHelper.WriteString(message.MessageId, buffer, output);

            WriteTags(flag, message, output);

            //记录当前指针位置
            int writerIndex = (int)output.Position;
            int bodyStart = writerIndex + 4;
            output.Position = bodyStart;
            //指针 预留4个位置开始写 Attrs
            SerializeMap(message.Attrs, buffer, output);

            int end = (int)output.Position;
            int bodyLen = end - bodyStart;
            int messageLength = end - messageStart;

            output.Position = writerIndex;
            //记录 body 的长度
            ByteBufHelper.WriteInt32(bodyLen, buffer, output);

            //write crc32
            output.Position = crcIndex;
            ByteBufHelper.WriteInt64(Crc32.GetCRC32(output, messageStart, messageLength), buffer, output);
            //指针重置到尾端
            output.Position = end;

        }

        void WriteTags(Flag flag, BaseMessage message, Stream output)
        {
            if (Flags.HasTag(flag))
            {
                var tags = message.GetTags();
                byte[] buffer = new byte[8];
                ByteBufHelper.WriteByte((byte)tags.Count, output);
                foreach (var item in tags)
                {
                    ByteBufHelper.WriteString(item, buffer, output);
                }
            }
        }

        private void SerializeMap(Hashtable map, byte[] lenBuffer, Stream output)
        {
            if (map == null || map.Count == 0) return;

            foreach (var item in map.Keys)
            {
                if (item == null || map[item] == null) continue;

                ByteBufHelper.WriteString(item, lenBuffer, output);
                ByteBufHelper.WriteString(map[item], lenBuffer, output);
            }
        }

        private BaseMessage ConvertTo(Message message)
        {
            return (BaseMessage)message;
        }
    }
}
