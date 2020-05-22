using System;
using System.Collections.Generic;

namespace Qunar.TC.Qmq.Client.Consumer
{
    internal class PullResult
    {
        private PullResult(bool success, List<Message> messages, Exception exception, string errorMessage)
        {
            Success = success;
            Messages = messages;
            Exception = exception;
            ErrorMessage = errorMessage;
        }

        public static PullResult Ok(List<Message> messages)
        {
            return new PullResult(true, messages, null, null);
        }

        public static PullResult Err(Exception exception, string errorMessage)
        {
            return new PullResult(false, null, exception, errorMessage);
        }

        public bool Success { get; }

        public List<Message> Messages { get; }

        public Exception Exception { get; }

        public string ErrorMessage { get; }
    }
}