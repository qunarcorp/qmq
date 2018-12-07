/*
 * Copyright 2018 Qunar
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
 * limitations under the License.com.qunar.pay.trade.api.card.service.usercard.UserCardQueryFacade
 */

package qunar.tc.qmq.delay;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;

import java.util.List;

public class ScheduleIndex {
    public static ByteBuf buildIndex(long scheduleTime, long offset, int size, long sequence) {
        ByteBuf index = PooledByteBufAllocator.DEFAULT.ioBuffer(8 + 8 + 4 + 8);
        index.writeLong(scheduleTime);
        index.writeLong(offset);
        index.writeInt(size);
        index.writeLong(sequence);
        return index;
    }

    public static long scheduleTime(ByteBuf index) {
        return index.getLong(0);
    }

    public static long offset(ByteBuf index) {
        return index.getLong(8);
    }

    public static int size(ByteBuf index) {
        return index.getInt(16);
    }

    public static long sequence(ByteBuf index) {
        return index.getLong(20);
    }

    public static void release(List<ByteBuf> resources) {
        for (ByteBuf resource : resources) {
            release(resource);
        }
    }

    public static void release(ByteBuf resource) {
        ReferenceCountUtil.safeRelease(resource);
    }
}
