package qunar.tc.qmq.producer.sender;

import qunar.tc.qmq.ClientType;

/**
 * @author zhenwei.liu
 * @since 2019-09-08
 */
public interface MessageSendContextManager {

    class MessageSendContext {
        private volatile String lastSentBroker;

        public String getLastSentBroker() {
            return lastSentBroker;
        }

        public MessageSendContext setLastSentBroker(String lastSentBroker) {
            this.lastSentBroker = lastSentBroker;
            return this;
        }
    }

    class ContextKey {
        private ClientType clientType;
        private String subject;
        private int partition;

        public ContextKey(ClientType clientType, String subject, int partition) {
            this.clientType = clientType;
            this.subject = subject;
            this.partition = partition;
        }

        public ClientType getClientType() {
            return clientType;
        }

        public String getSubject() {
            return subject;
        }

        public int getPartition() {
            return partition;
        }
    }

    MessageSendContext getContext(ContextKey key);
}
