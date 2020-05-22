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

package qunar.tc.qmq.task.monitor;

import qunar.tc.qmq.metrics.Metrics;

/**
 * @author kelly.li
 * @date 2015-10-16
 */
public class Qmon {
    public static void lostLeaderError() {
        Metrics.counter("lostLeaderError").inc();
    }

    public static void doElectException() {
        Metrics.counter("electException").inc();
    }

    public static void leaderRenewedRentTaskException() {
        Metrics.counter("leaderRenewedRentTaskException").inc();
    }

    public static void initSendMessageTasksCountInc() {
        Metrics.counter("initSendMessageTasksCount").inc();
    }

    public static void initSendMessageTasksFailCountInc() {
        Metrics.counter("initSendMessageTasksFailCount").inc();
    }

    public static void sendMessageTaskInvokeCountInc() {
        Metrics.counter("sendMessageTaskInvokeCount").inc();
    }

    public static void sendMessageTaskInvokeFailCountInc() {
        Metrics.counter("sendMessageTaskInvokeFailCount").inc();
    }

    public static void taskSendMsgErrorCountInc(String subject, String jdbcUrl) {
        Metrics.counter("taskSendMsgErrorCount", new String[]{"subject", "jdbcUrl"}, new String[]{subject, jdbcUrl}).inc();
    }

    public static void sendMesssagesSuccessCountInc(String subject, String jdbcUrl) {
        Metrics.counter("sendNewqmqMesssagesSuccessCount", new String[]{"subject", "jdbcUrl"}, new String[]{subject, jdbcUrl}).inc();
    }

    public static void sendNewqmqMesssagesFailedCountInc(String subject, String jdbcUrl) {
        Metrics.counter("sendNewqmqMesssagesFailedCount", new String[]{"subject", "jdbcUrl"}, new String[]{subject, jdbcUrl}).inc();
    }

    public static void noSendMsgCountInc(String jdbcUrl, int n) {
        Metrics.counter("noSendMsgCount", new String[]{"jdbcUrl"}, new String[]{jdbcUrl}).inc(n);
    }

    public static void invalidMsgCountInc(String jdbcUrl) {
        Metrics.counter("invalidMsgCount", new String[]{"jdbcUrl"}, new String[]{jdbcUrl}).inc();
    }
}
