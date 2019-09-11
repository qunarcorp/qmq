package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.CompositePullClient;
import qunar.tc.qmq.ConsumeMode;
import qunar.tc.qmq.PullEntry;
import qunar.tc.qmq.StatusSource;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author zhenwei.liu
 * @since 2019-09-02
 */
public class CompositePullEntry<T extends PullEntry> extends AbstractPullClient implements PullEntry, CompositePullClient<T> {

    private List<T> pullEntries;

    public CompositePullEntry(String subject, String group, int version, List<T> pullEntries) {
        super(subject, group, "", version);
        this.pullEntries = pullEntries;
    }

    @Override
    public void online(StatusSource statusSource) {
        pullEntries.forEach(pe -> pe.online(statusSource));
    }

    @Override
    public void offline(StatusSource statusSource) {
        pullEntries.forEach(pe -> pe.offline(statusSource));
    }

    @Override
    public void startPull(ExecutorService executor) {
        pullEntries.forEach(pe -> startPull(executor));
    }

    @Override
    public void destroy() {
        pullEntries.forEach(PullEntry::destroy);
    }

    @Override
    public List<T> getComponents() {
        return pullEntries;
    }
}
