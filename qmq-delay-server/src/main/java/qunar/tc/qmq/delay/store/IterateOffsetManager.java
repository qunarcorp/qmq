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

package qunar.tc.qmq.delay.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import qunar.tc.qmq.common.JsonHolder;
import qunar.tc.qmq.store.CheckpointStore;
import qunar.tc.qmq.store.FlushHook;
import qunar.tc.qmq.store.PeriodicFlushService;
import qunar.tc.qmq.store.Serde;

import java.io.IOException;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 17:31
 */
public class IterateOffsetManager {
    private static final String ITERATE_OFFSET_FILE = "message_log_iterate_checkpoint.json";
    private static final int DEFAULT_FLUSH_INTERVAL = 10 * 1000;
    private static final ObjectMapper MAPPER = JsonHolder.getMapper();

    private final CheckpointStore<Long> offsetCheckpointStore;
    private final FlushHook flushHook;

    private volatile long iterateOffset = 0;

    public IterateOffsetManager(String checkpointStorePath, FlushHook hook) {
        this.offsetCheckpointStore = new CheckpointStore<>(checkpointStorePath, ITERATE_OFFSET_FILE, new IterateCheckpointSerde());
        Long offset = this.offsetCheckpointStore.loadCheckpoint();
        if (null != offset) {
            this.iterateOffset = offset;
        }
        this.flushHook = hook;
    }

    public synchronized void updateIterateOffset(long offset) {
        if (offset > iterateOffset) {
            this.iterateOffset = offset;
        }
    }

    public long getIterateOffset() {
        return iterateOffset;
    }

    public PeriodicFlushService.FlushProvider getFlushProvider() {
        return new PeriodicFlushService.FlushProvider() {

            @Override
            public int getInterval() {
                return DEFAULT_FLUSH_INTERVAL;
            }

            @Override
            public void flush() {
                flushHook.beforeFlush();
                offsetCheckpointStore.saveCheckpoint(iterateOffset);
            }
        };
    }

    private static final class IterateCheckpointSerde implements Serde<Long> {

        @Override
        public byte[] toBytes(Long value) {
            try {
                return MAPPER.writeValueAsBytes(value);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("serialize message log iterate offset checkpoint failed.", e);
            }
        }

        @Override
        public Long fromBytes(byte[] data) {
            try {
                return MAPPER.readValue(data, Long.class);
            } catch (IOException e) {
                throw new RuntimeException("deserialize message log iterate offset checkpoint failed.", e);
            }
        }
    }
}
