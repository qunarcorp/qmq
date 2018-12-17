package qunar.tc.qmq.task;

import qunar.tc.qmq.task.store.LeaderElectionDao;

class Tasks {

    private LeaderElection leaderElection;

    public Tasks(String namespace, TaskManager taskManager, LeaderElectionDao leaderElectionDao) {
        leaderElection = new LeaderElection("qmq_watchdog_" + namespace, new LeaderChangedListener() {
            @Override
            public void own() {
                taskManager.start();
            }

            @Override
            public void lost() {
                taskManager.destroy();
            }
        }, leaderElectionDao);
    }

    public void start() {
        leaderElection.startElect();
    }

    public void destroy() {
        if (leaderElection != null) {
            leaderElection.destroy();
        }
    }
}
