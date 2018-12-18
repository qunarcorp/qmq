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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.task.monitor.Qmon;
import qunar.tc.qmq.task.store.LeaderElectionDao;
import qunar.tc.qmq.utils.NetworkUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class LeaderElection {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElection.class);

    private static final int INITIAL_DELAY_SECOND = 0;
    private static final int PERIOD_SECOND = 1;
    private static final long RENEWEDRENT_FAILED_COUNT_THRESHOLD = 5;
    private final long MAX_LEADER_ERROR_TIME_IN_MILLISECONDS = RENEWEDRENT_FAILED_COUNT_THRESHOLD * PERIOD_SECOND * 1000;
    private static final int ZERO = 0;
    private final AtomicInteger renewedRentFailedCount = new AtomicInteger(ZERO);

    private final String leaderElectionKey;
    private final LeaderChangedListener listener;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final AtomicBoolean isOnline = new AtomicBoolean(true);
    private final Object lock = new Object();

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("leader_election"));
    private final ScheduledExecutorService renewedRentExecutorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("leader_renewedRent"));

    private ScheduledFuture<?> leaderRenewedRentFuture = null;

    private final LeaderElectionDao leaderElectionDao;

    public LeaderElection(final String leaderElectionKey, final LeaderChangedListener listener, LeaderElectionDao leaderElectionDao) {
        this.leaderElectionKey = leaderElectionKey;
        this.listener = listener;
        this.leaderElectionDao = leaderElectionDao;
    }

    /**
     * 开始leader选举
     */
    public void startElect() {
        //选举任务
        executorService.scheduleAtFixedRate(() -> {
            synchronized (lock) {
                doElect();
            }
        }, INITIAL_DELAY_SECOND, PERIOD_SECOND, TimeUnit.SECONDS);
    }

    private void doElect() {
        try {
            LeaderElectionRecord recordInDB = leaderElectionDao.queryByName(leaderElectionKey);
            if (recordInDB == null) {
                //第一次执行, leader_election表中还没有name为:[" + leaderElectionKey + "]的记录
                if (initDbRecordAndOwnLeader()) {
                    LOGGER.info("[LeaderElection] {}成为leader", getCurrentNode());
                    return;
                }
            }

            //处理直接更新db的情况
            if (checkMeIsLeaderFromDb()) {
                if (isLeader.get()) {
                    //自己还是leader, 直接return
                    return;
                }
                //数据库中是leader, 但自己不认为自己是
                processWhenOwnLeader();
                return;
            } else {
                //数据库中不是leader, 但自己还认为自己是
                if (isLeader.get()) {
                    LOGGER.info("[LeaderElection] db中自己已经不是leader了，机器{}放弃leader", getCurrentNode());
                    giveUpLeaderImmediately();
                }
            }

            recordInDB = leaderElectionDao.queryByName(leaderElectionKey);
            int numberOfRowAffected = leaderElectionDao.elect(leaderElectionKey, getCurrentNode(), recordInDB.getLastSeenActive(), MAX_LEADER_ERROR_TIME_IN_MILLISECONDS);
            if (numberOfRowAffected > 0) {
                processWhenOwnLeader();
            }
        } catch (Exception e) {
            LOGGER.error("[LeaderElection] doElect异常", e.getMessage(), e);
            //leader出现错误(比如数据库访问异常的时候), 主动放弃leader, 避免自己一直出错, 但是又不放弃leader的情况
            Qmon.doElectException();
            giveUpLeaderWhenConditionMatch();
        }
    }

    private String getCurrentNode() {
        return NetworkUtils.getLocalHostname();
    }

    /**
     * 初始化db记录, 并且获得leader
     */
    private boolean initDbRecordAndOwnLeader() {
        try {
            int numberOfRowAffected = leaderElectionDao.initLeader(leaderElectionKey, getCurrentNode());
            if ((numberOfRowAffected > 0)) {
                processWhenOwnLeader();
                return true;
            }
        } catch (DuplicateKeyException e) {
            //说明表中已经有记录了 ignore异常
        }
        return false;
    }

    /**
     * 检查数据库记录,判断自己是否是leader
     */
    private boolean checkMeIsLeaderFromDb() {
        return renewedRent();
    }

    /**
     * leader进行续租
     */
    private boolean renewedRent() {
        int numberOfRowAffected = leaderElectionDao.renewedRent(leaderElectionKey, getCurrentNode());
        return numberOfRowAffected > 0;
    }

    /**
     * 主动放弃leader
     */
    private void giveUpLeaderImmediately() {
        processWhenLostLeader();
    }

    private void giveUpLeaderWhenConditionMatch() {
        if (renewedRentFailedCount.incrementAndGet() >= RENEWEDRENT_FAILED_COUNT_THRESHOLD) {
            LOGGER.info("[LeaderElection] leader机器{}重试{}次后准备放弃leader", getCurrentNode(), RENEWEDRENT_FAILED_COUNT_THRESHOLD);
            giveUpLeaderImmediately();
        }
    }

    /**
     * 启动leader续租任务
     */
    private ScheduledFuture<?> startLeaderRenewedRentTask() {
        LOGGER.info("[LeaderElection] leader机器{}启动续租线程", getCurrentNode());
        return renewedRentExecutorService.scheduleAtFixedRate(() -> {
            try {
                if (isOnline.get()) {
                    if (!renewedRent()) {
                        synchronized (lock) {
                            LOGGER.info("[LeaderElection] leader机器{}续租失败，立即放弃leader", getCurrentNode());
                            //续租没成功, 立即放弃leader
                            giveUpLeaderImmediately();
                        }
                    } else {
                        LOGGER.debug("[LeaderElection] leader机器{}续租成功", getCurrentNode());
                        renewedRentFailedCount.set(ZERO);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("[LeaderElection] leader机器{}续租线程执行异常{}", getCurrentNode(), e.getMessage(), e);
                //异常情况下, 最多重试RENEWEDRENT_FAILED_COUNT_THRESHOLD次
                Qmon.leaderRenewedRentTaskException();
                synchronized (lock) {
                    giveUpLeaderWhenConditionMatch();
                }
            }
        }, INITIAL_DELAY_SECOND, PERIOD_SECOND, TimeUnit.SECONDS);
    }

    private void processWhenOwnLeader() {
        if (isLeader.compareAndSet(false, true) && isOnline.get()) {
            LOGGER.info("[LeaderElection] 机器{}成为leader", getCurrentNode());
            //leader续租任务
            leaderRenewedRentFuture = startLeaderRenewedRentTask();
            renewedRentFailedCount.set(ZERO);
            try {
                listener.own();
            } catch (Exception e) {
                LOGGER.error("[LeaderElection] 执行leader own()出现异常", e);
            }
        }
    }

    private void processWhenLostLeader() {
        if (isLeader.compareAndSet(true, false)) {
            LOGGER.info("[LeaderElection] 机器{}放弃leader", getCurrentNode());
            try {
                listener.lost();
            } catch (Exception e) {
                LOGGER.error("[LeaderElection] 执行leader lost()出现异常", e);
                Qmon.lostLeaderError();
            }
            if (leaderRenewedRentFuture != null) {
                leaderRenewedRentFuture.cancel(true);
                leaderRenewedRentFuture = null;
            }
            renewedRentFailedCount.set(ZERO);
            try {
                //置空db中的node, 以便其余Server可以尽快的选出leader
                leaderElectionDao.setLeaderEmptyIfLeaderIsMine(leaderElectionKey, getCurrentNode());
            } catch (Exception e) {
                LOGGER.error("[LeaderElection] 置空db中的node失败", e.getMessage(), e);
            }
        }
    }

    public void destroy() {
        try {
            executorService.shutdownNow();
        } catch (Exception ignore) {

        }
        try {
            renewedRentExecutorService.shutdownNow();
        } catch (Exception ignore) {

        }
    }
}

