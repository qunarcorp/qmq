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

package qunar.tc.qmq.delay.monitor;

import qunar.tc.qmq.metrics.Metrics;

import java.util.concurrent.TimeUnit;

/**
 * 需要统一一下，TODO server中的QMon抽离到公共模块
 *
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-26 14:49
 */
public class QMon {
    private static final String[] EMPTY = new String[]{};

    private static final String[] SUBJECT_ARRAY = new String[]{"subject"};
    private static final String[] BROKER_ARRAY = new String[]{"broker"};
    private static final String[] LOGTYPE_ARRAY = new String[]{"logType"};
    private static final String[] BROKER_GROUP_SUBJECT_ARRAY = new String[]{"group", "subject"};

    public static void scheduleDispatch() {
        Metrics.counter("scheduleDispatch", EMPTY, EMPTY).inc();
    }

    public static void rejectReceivedMessageCountInc(String subject) {
        countInc("rejectReceivedMessageCount", subject);
    }

    public static void delayBrokerReadOnlyMessageCountInc(String subject) {
        countInc("delayBrokerReadOnlyMessageCount", subject);
    }

    public static void nettySendMessageFailCount(String subject, String groupName) {
        countInc("nettySendMessageFailCount", BROKER_GROUP_SUBJECT_ARRAY, new String[]{groupName, subject});
    }

    public static void appendFailed(String subject) {
        countInc("appendMessageLogFailCount", subject);
    }

    public static void receivedMessagesCountInc(String subject) {
        final String[] values = {subject};
        countInc("receivedMessagesCount", SUBJECT_ARRAY, values);
        Metrics.meter("receivedMessagesEx", SUBJECT_ARRAY, values).mark();
    }

    public static void produceTime(String subject, long time) {
        Metrics.timer("produceTime", SUBJECT_ARRAY, new String[]{subject}).update(time, TimeUnit.MILLISECONDS);
    }

    public static void delayTime(String group, String subject, long time) {
        Metrics.timer("delayTime", BROKER_GROUP_SUBJECT_ARRAY, new String[]{group, subject}).update(time, TimeUnit.MILLISECONDS);
    }

    public static void receivedRetryMessagesCountInc(String subject) {
        final String[] values = {subject};
        countInc("receivedRetryMessagesCount", SUBJECT_ARRAY, values);
    }

    public static void delayBrokerSendMsgCount(String groupName, String subject) {
        countInc("delayBrokerSendMsgCount", BROKER_GROUP_SUBJECT_ARRAY, new String[]{groupName, subject});
    }

    private static void countInc(String name, String subject) {
        countInc(name, SUBJECT_ARRAY, new String[]{subject});
    }

    public static void appendTimer(String subject, long time) {
        Metrics.timer("appendMessageLogCostTime", SUBJECT_ARRAY, new String[]{subject}).update(time, TimeUnit.MILLISECONDS);
    }

    private static void countInc(String name, String[] tags, String[] values) {
        Metrics.counter(name, tags, values).inc();
    }

    public static void loadMsgTime(long time) {
        Metrics.timer("loadMsgTime", EMPTY, EMPTY).update(time, TimeUnit.MILLISECONDS);
    }

    public static void sendMsgTime(String broker, long time) {
        Metrics.timer("sendMsgTime", BROKER_ARRAY, new String[]{broker}).update(time, TimeUnit.MILLISECONDS);
    }

    public static void receiveFailedCuntInc(String subject) {
        countInc("receivedFailedCount", subject);
    }

    public static void overDelay(String subject) {
        countInc("overDelayMessage", subject);
    }

    public static void slaveSyncLogOffset(String logType, long diff) {
        Metrics.timer("slaveSyncLogOffsetLag", LOGTYPE_ARRAY, new String[]{logType}).update(diff, TimeUnit.MILLISECONDS);
    }

    public static void receivedIllegalSubjectMessagesCountInc(String subject) {
        final String[] values = {subject};
        countInc("receivedIllegalSubjectMessagesCount", SUBJECT_ARRAY, values);
    }

    public static void loadSegmentFailed() {
        countInc("loadScheduleSegmentFailed", EMPTY, EMPTY);
    }

    public static void appendFailedByMessageIllegal(String subject) {
        countInc("appendMessageFailedByIllegal", subject);
    }
}
