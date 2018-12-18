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

package qunar.tc.qmq.meta.monitor;

import qunar.tc.qmq.metrics.Metrics;

import static qunar.tc.qmq.metrics.MetricsConstants.SUBJECT_ARRAY;

/**
 * @author yunfeng.yang
 * @since 2017/7/10
 */
public final class QMon {

    public static void brokerRegisterCountInc(String groupName, int requestType) {
        Metrics.counter("brokerRegisterCount", new String[]{"groupName", "requestType"}, new String[]{groupName, String.valueOf(requestType)}).inc();
    }

    public static void brokerDisconnectedCountInc(String groupName) {
        Metrics.counter("brokerDisconnectedCount", new String[]{"groupName"}, new String[]{groupName}).inc();
    }

    public static void clientRegisterCountInc(String subject, int clientTypeCode) {
        Metrics.counter("clientRegisterCount", new String[]{"subject", "clientTypeCode"}, new String[]{subject, String.valueOf(clientTypeCode)}).inc();
    }

    public static void clientSubjectRouteCountInc(String subject) {
        Metrics.counter("clientSubjectRouteCount", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

    public static void clientRefreshMetaInfoCountInc(String subject) {
        Metrics.counter("clientRefreshMetaInfoCount", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

    public static void subjectInfoNotFound(String subject) {
        Metrics.counter("subjectInfoNotFoundCount", SUBJECT_ARRAY, new String[]{subject}).inc();
    }

}
