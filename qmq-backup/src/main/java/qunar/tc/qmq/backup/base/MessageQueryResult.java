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

package qunar.tc.qmq.backup.base;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * User: zhaohuiyu Date: 3/22/14 Time: 10:29 PM
 */
public class MessageQueryResult<T> implements Serializable {
    private static final long serialVersionUID = -6106414829068194397L;

    private List<T> list = Lists.newArrayList();
    private Serializable next;

    public MessageQueryResult() {
        super();
    }

    public void setList(List<T> list) {
        this.list = list;
    }

    public List<T> getList() {
        return list;
    }

    public void setNext(Serializable next) {
        this.next = next;
    }

    public Serializable getNext() {
        return next;
    }

    public static class MessageMeta {
        private final String subejct;
        private final String messageId;
        private final long sequence;
        private final long createTime;
        private final String brokerGroup;

        public MessageMeta(String subject, String messageId, long sequence, long createTime, String brokerGroup) {
            this.subejct = subject;
            this.messageId = messageId;
            this.sequence = sequence;
            this.createTime = createTime;
            this.brokerGroup = brokerGroup;
        }

        public String getSubejct() {
            return subejct;
        }

        public String getMessageId() {
            return messageId;
        }

        public long getSequence() {
            return sequence;
        }

        public long getCreateTime() {
            return createTime;
        }

        public String getBrokerGroup() {
            return brokerGroup;
        }
    }

}
