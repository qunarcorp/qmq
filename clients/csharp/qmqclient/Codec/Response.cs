using System;

namespace Qunar.TC.Qmq.Client.Codec
{
    internal class Response
    {
        public static readonly TimeoutException TimeoutException = new TimeoutException();

        public const byte Ok = 20;
        public const byte Timout = 30;
        public const byte Error = 40;

        public static readonly Response NeedMore = new Response(-1);
        public static readonly Response Timeout;

        static Response()
        {
            Timeout = new Response(-1, Timout) { result = TimeoutException };
        }

        private readonly long id;
        private object result;
        private byte status = Ok;

        private bool isEvent = false;

        private string errorMessage;

        public Response(long id)
        {
            this.id = id;
        }

        public Response(long id, byte status)
        {
            this.id = id;
            this.status = status;
        }

        public long Id
        {
            get
            {
                return id;
            }
        }

        public object Result
        {
            get
            {
                return result;
            }

            set
            {
                this.result = value;
            }
        }

        public byte Status
        {
            get
            {
                return this.status;
            }
            set
            {
                this.status = value;
            }
        }

        public string ErrorMessage
        {
            get
            {
                return errorMessage;
            }

            set
            {
                errorMessage = value;
            }
        }

        public bool IsOk()
        {
            return status == Ok;
        }

        public bool Event
        {
            get
            {
                return this.isEvent;
            }

            set
            {
                this.isEvent = value;
            }
        }
    }
}

