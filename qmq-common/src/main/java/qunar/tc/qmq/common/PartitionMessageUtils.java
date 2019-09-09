package qunar.tc.qmq.common;

import qunar.tc.qmq.Message;
import qunar.tc.qmq.base.BaseMessage;

/**
 * @author zhenwei.liu
 * @since 2019-08-19
 */
public class PartitionMessageUtils {

    public static String getRealSubject(Message message) {
        String subjectSuffix = message.getStringProperty(BaseMessage.keys.qmq_subjectSuffix.name());
        if (subjectSuffix != null) {
            String subject = message.getSubject();
            return subject.replace(subjectSuffix, "");
        }
        return message.getSubject();
    }
}
