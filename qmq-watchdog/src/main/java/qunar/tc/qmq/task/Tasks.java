/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
