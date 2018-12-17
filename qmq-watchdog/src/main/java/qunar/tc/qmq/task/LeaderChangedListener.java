package qunar.tc.qmq.task;

interface LeaderChangedListener {
    void own();

    void lost();
}
