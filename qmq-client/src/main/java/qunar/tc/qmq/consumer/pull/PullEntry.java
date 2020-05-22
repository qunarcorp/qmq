package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.common.StatusSource;

/**
 * @author zhenwei.liu
 * @since 2019-10-15
 */
public interface PullEntry {

    PullEntry EMPTY_PULL_ENTRY = new PullEntry() {

        @Override
        public String getSubject() {
            return null;
        }

        @Override
        public String getConsumerGroup() {
            return null;
        }

        @Override
        public void online(StatusSource src) {
        }

        @Override
        public void offline(StatusSource src) {
        }

        @Override
        public void startPull() {

        }

        @Override
        public void destroy() {

        }
    };

    String getSubject();

    String getConsumerGroup();

    void online(StatusSource src);

    void offline(StatusSource src);

    void startPull();

    void destroy();

}
