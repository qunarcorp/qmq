using System;

namespace Qunar.TC.Qmq.Client.Util
{
    internal static class RetrySubjectUtils
    {
        private const string SubjectDelimiter = "%";
        private const string RetrySubjectPrefix = "%RETRY";
        private const string DeadRetrySubjectPrefix = "%DEAD_RETRY";

        public static string BuildRetrySubject(string subject, string group)
        {
            return string.Join(SubjectDelimiter, RetrySubjectPrefix, subject, group);
        }

        public static string BuildDeadRetrySubject(string subject, string group)
        {
            return string.Join(SubjectDelimiter, DeadRetrySubjectPrefix, subject, group);
        }

        public static bool IsRetrySubject(string subject)
        {
            return subject.StartsWith(RetrySubjectPrefix);
        }

        public static bool IsDeadRetrySubject(string subject)
        {
            return subject.StartsWith(DeadRetrySubjectPrefix);
        }

        public static string RealSubject(string subject)
        {
            if (!IsRetrySubject(subject) && !IsDeadRetrySubject(subject))
            {
                return subject;
            }

            var parts = subject.Split(SubjectDelimiter.ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
            return parts.Length != 3 ? subject : parts[1];
        }
    }
}