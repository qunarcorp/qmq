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

package qunar.tc.qmq.delay.store.model;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.base.MessageHeader;
import qunar.tc.qmq.base.RawMessage;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-12 14:35
 */
public class RawMessageExtend extends RawMessage {
    private long scheduleTime;

    public RawMessageExtend(MessageHeader header, ByteBuf body, int size, long scheduleTime) {
        super(header, body, size);
        this.scheduleTime = scheduleTime;
    }

    public long getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(long scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    @Override
    public String toString() {
        return "RawMessageExtend{" +
                "scheduleTime=" + scheduleTime +
                '}';
    }
}
