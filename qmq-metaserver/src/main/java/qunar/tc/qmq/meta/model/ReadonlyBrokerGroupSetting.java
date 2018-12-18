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

package qunar.tc.qmq.meta.model;

/**
 * @author keli.wang
 * @since 2018/7/30
 */
public class ReadonlyBrokerGroupSetting {
    private final String subject;
    private final String brokerGroup;

    public ReadonlyBrokerGroupSetting(String subject, String brokerGroup) {
        this.subject = subject;
        this.brokerGroup = brokerGroup;
    }

    public String getSubject() {
        return subject;
    }

    public String getBrokerGroup() {
        return brokerGroup;
    }

    @Override
    public String toString() {
        return "ReadonlyBrokerGroup{" +
                "subject='" + subject + '\'' +
                ", brokerGroup='" + brokerGroup + '\'' +
                '}';
    }
}
