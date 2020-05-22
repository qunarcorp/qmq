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

package qunar.tc.qmq.store.action;

import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.ActionType;

/**
 * @author yunfeng.yang
 * @since 2017/8/28
 */
public class RangeAckAction implements Action {
    private final String subject;
    private final String group;
    private final String consumerId;
    private final long timestamp;

    private final long firstSequence;
    private final long lastSequence;

    public RangeAckAction(String subject, String group, String consumerId, long timestamp, long firstSequence, long lastSequence) {
        this.subject = subject;
        this.group = group;
        this.consumerId = consumerId;
        this.timestamp = timestamp;

        this.firstSequence = firstSequence;
        this.lastSequence = lastSequence;
    }

    @Override
    public ActionType type() {
        return ActionType.RANGE_ACK;
    }

    @Override
    public String subject() {
        return subject;
    }

    @Override
    public String group() {
        return group;
    }

    @Override
    public String consumerId() {
        return consumerId;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    public long getFirstSequence() {
        return firstSequence;
    }

    public long getLastSequence() {
        return lastSequence;
    }

    @Override
    public String toString() {
        return "RangeAckAction{" +
                "subject='" + subject + '\'' +
                ", group='" + group + '\'' +
                ", consumerId='" + consumerId + '\'' +
                ", firstSequence=" + firstSequence +
                ", lastSequence=" + lastSequence +
                '}';
    }
}
