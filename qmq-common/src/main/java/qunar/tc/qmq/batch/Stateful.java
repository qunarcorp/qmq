package qunar.tc.qmq.batch;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public interface Stateful<State> {

    State getState();

    void setState(State state);

    boolean compareAndSetState(State oldState, State newState);

    void reset();
}
