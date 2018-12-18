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

package qunar.tc.qmq.protocol.consumer;

/**
 * @author yiqun.fan create on 17-8-24.
 */
public class AckRequest {
    private static final byte UNSET = -1;
    private String subject;
    private String group;
    private String consumerId;
    private long pullOffsetBegin;
    private long pullOffsetLast;
    private byte isBroadcast = UNSET;

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(String consumerId) {
        this.consumerId = consumerId;
    }

    public long getPullOffsetBegin() {
        return pullOffsetBegin;
    }

    public void setPullOffsetBegin(long pullOffsetBegin) {
        this.pullOffsetBegin = pullOffsetBegin;
    }

    public long getPullOffsetLast() {
        return pullOffsetLast;
    }

    public void setPullOffsetLast(long pullOffsetLast) {
        this.pullOffsetLast = pullOffsetLast;
    }

    public void setBroadcast(byte isBroadcast) {
        this.isBroadcast = isBroadcast;
    }

    public byte isBroadcast() {
        return isBroadcast;
    }
}
