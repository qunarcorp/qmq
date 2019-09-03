package qunar.tc.qmq;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public interface CompositePullClient<T> extends PullClient {

    List<T> getComponents();
}
