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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.JsonHolder;
import qunar.tc.qmq.delay.config.StoreConfiguration;
import qunar.tc.qmq.store.Serde;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-16 14:59
 */
public class ScheduleLogValidatorSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduleLogValidatorSupport.class);

    private static final String SCHEDULE_OFFSET_CHECKPOINT = "schedule_offset_checkpoint.json";

    private static final ScheduleOffsetSerde SERDE = new ScheduleOffsetSerde();
    private static final ObjectMapper MAPPER = JsonHolder.getMapper();

    private static ScheduleLogValidatorSupport SUPPORT;

    private final StoreConfiguration config;

    private ScheduleLogValidatorSupport(StoreConfiguration config) {
        this.config = config;
    }

    public static ScheduleLogValidatorSupport getSupport(StoreConfiguration config) {
        if (null == SUPPORT) {
            SUPPORT = new ScheduleLogValidatorSupport(config);
        }

        return SUPPORT;
    }

    public void saveScheduleOffsetCheckpoint(Map<Long, Long> offsets) {
        ensureDir(config.getScheduleOffsetCheckpointPath());
        final byte[] data = SERDE.toBytes(offsets);
        Preconditions.checkState(data != null, "Serialized checkpoint data should not be null.");
        if (data.length == 0) {
            return;
        }

        final File checkpoint = new File(config.getScheduleOffsetCheckpointPath(), SCHEDULE_OFFSET_CHECKPOINT);
        try {
            Files.write(data, checkpoint);
        } catch (IOException e) {
            LOGGER.error("write data into schedule checkpoint file failed. file={}", checkpoint, e);
            throw new RuntimeException("write checkpoint data failed.", e);
        }
    }

    private void ensureDir(final String storePath) {
        final File store = new File(storePath);
        if (store.exists()) {
            return;
        }

        final boolean success = store.mkdirs();
        if (!success) {
            throw new RuntimeException("Failed create path " + storePath);
        }
        LOGGER.info("Create checkpoint store {} success.", storePath);
    }

    public Map<Long, Long> loadScheduleOffsetCheckpoint() {
        File file = new File(config.getScheduleOffsetCheckpointPath(), SCHEDULE_OFFSET_CHECKPOINT);
        if (!file.exists()) {
            return new HashMap<>(0);
        }

        try {
            final byte[] data = Files.toByteArray(file);
            if (data != null && data.length == 0) {
                if (!file.delete()) throw new RuntimeException("remove checkpoint error. filename=" + file);
                return new HashMap<>(0);
            }
            Map<Long, Long> offsets = SERDE.fromBytes(data);
            if (null == offsets || !file.delete()) {
                throw new RuntimeException("Load checkpoint error. filename=" + file);
            }

            return offsets;
        } catch (IOException e) {
            LOGGER.error("Load checkpoint file failed.", e);
        }

        throw new RuntimeException("Load checkpoint failed. filename=" + file);
    }

    private static class ScheduleOffsetSerde implements Serde<Map<Long, Long>> {

        @Override
        public byte[] toBytes(Map<Long, Long> value) {
            try {
                return MAPPER.writeValueAsBytes(value);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("serialize schedule offset failed.", e);
            }
        }

        @Override
        public Map<Long, Long> fromBytes(byte[] data) {
            try {
                return MAPPER.readValue(data, new TypeReference<Map<Long, Long>>() {
                });
            } catch (IOException e) {
                throw new RuntimeException("deserialize schedule offset checkpoint failed.", e);
            }
        }
    }

}
