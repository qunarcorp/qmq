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

import com.google.common.base.Preconditions;
import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.ActionType;

/**
 * @author keli.wang
 * @since 2017/8/20
 */
public class PullAction implements Action {
    private final String subject;
    private final String group;
    private final String consumerId;
    private final long timestamp;
    private final boolean isExclusiveConsume;

    //first sequence of pull log
    private final long firstSequence;

    //last sequence of pull log
    private final long lastSequence;

    //fist sequence of consumer log
    private final long firstMessageSequence;

    //last sequence of consumer log
    private final long lastMessageSequence;

    public PullAction(final String subject, final String group, final String consumerId, long timestamp, boolean isExclusiveConsume,
                      long firstSequence, long lastSequence,
                      long firstMessageSequence, long lastMessageSequence) {
        Preconditions.checkArgument(lastSequence - firstSequence == lastMessageSequence - firstMessageSequence);

        this.subject = subject;
        this.group = group;
        this.consumerId = consumerId;
        this.timestamp = timestamp;
        this.isExclusiveConsume = isExclusiveConsume;

        this.firstSequence = firstSequence;
        this.lastSequence = lastSequence;

        this.firstMessageSequence = firstMessageSequence;
        this.lastMessageSequence = lastMessageSequence;
    }

    @Override
    public ActionType type() {
        return ActionType.PULL;
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

    public boolean isExclusiveConsume() {
        return isExclusiveConsume;
    }

    /**
     * 在pull log中第一个偏移
     */
    public long getFirstSequence() {
        return firstSequence;
    }

    /**
     * 在pull log中最后一个偏移
     */
    public long getLastSequence() {
        return lastSequence;
    }

    /**
     * 在consuemr log中第一个偏移
     */
    public long getFirstMessageSequence() {
        return firstMessageSequence;
    }

    /**
     * 在consumer log中最后一个偏移
     */
    public long getLastMessageSequence() {
        return lastMessageSequence;
    }

    @Override
    public String toString() {
        return "PullAction{" +
                "subject='" + subject + '\'' +
                ", group='" + group + '\'' +
                ", consumerId='" + consumerId + '\'' +
                ", isExclusiveConsume=" + isExclusiveConsume +
                ", firstSequence=" + firstSequence +
                ", lastSequence=" + lastSequence +
                ", firstMessageSequence=" + firstMessageSequence +
                ", lastMessageSequence=" + lastMessageSequence +
                '}';
    }
}
