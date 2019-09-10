package qunar.tc.qmq.batch;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public abstract class StatefulSendMessageExecutor implements SendMessageExecutor<StatefulSendMessageExecutor.Status>, Stateful<StatefulSendMessageExecutor.Status> {

    public enum Status {
        IDLE, RUNNING
    }

    private AtomicReference<Status> status = new AtomicReference<>(Status.IDLE);

    @Override
    public Status getState() {
        return status.get();
    }

    @Override
    public void setState(Status status) {
        this.status.set(status);
    }

    @Override
    public boolean compareAndSetState(Status oldState, Status newState) {
        return this.status.compareAndSet(oldState, newState);
    }
}
