package qunar.tc.qmq.store;

public interface LogVisitor<Record> {
    LogVisitorRecord<Record> nextRecord();

    int visitedBufferSize();
}
