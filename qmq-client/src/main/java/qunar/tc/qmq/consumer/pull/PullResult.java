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

package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerGroupInfo;

import java.util.List;

/**
 * @author yiqun.fan create on 17-8-18.
 */
class PullResult {
    private final short responseCode;
    private final List<BaseMessage> messages;
    private final BrokerGroupInfo brokerGroup;

    public PullResult(short responseCode, List<BaseMessage> messages, BrokerGroupInfo brokerGroup) {
        this.responseCode = responseCode;
        this.messages = messages;
        this.brokerGroup = brokerGroup;
    }

    public short getResponseCode() {
        return responseCode;
    }

    public List<BaseMessage> getMessages() {
        return messages;
    }

    public BrokerGroupInfo getBrokerGroup() {
        return brokerGroup;
    }
}
