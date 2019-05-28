package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2019-01-15
 */
public class LogVisitorRecord<T> {
    private final RecordType type;
    private final T data;

    private LogVisitorRecord(final RecordType type, final T data) {
        this.type = type;
        this.data = data;
    }

    public static <T> LogVisitorRecord<T> noMore() {
        return new LogVisitorRecord<>(RecordType.NO_MORE, null);
    }

    public static <T> LogVisitorRecord<T> empty() {
        return new LogVisitorRecord<>(RecordType.EMPTY, null);
    }

    public static <T> LogVisitorRecord<T> data(final T data) {
        return new LogVisitorRecord<>(RecordType.DATA, data);
    }

    public boolean isNoMore() {
        return type == RecordType.NO_MORE;
    }

    public boolean hasData() {
        return type == RecordType.DATA;
    }

    public T getData() {
        return data;
    }

    public enum RecordType {
        NO_MORE, EMPTY, DATA
    }
}
