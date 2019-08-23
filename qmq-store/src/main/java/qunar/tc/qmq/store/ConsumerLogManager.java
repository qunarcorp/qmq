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

package qunar.tc.qmq.store;

import static com.google.common.base.CharMatcher.BREAKING_WHITESPACE;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.JsonHolder;
import qunar.tc.qmq.monitor.QMon;

/**
 * @author keli.wang
 * @since 2017/8/19
 */
public class ConsumerLogManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerLogManager.class);

    private static final ObjectMapper MAPPER = JsonHolder.getMapper();

    private final StorageConfig config;

    private final ConcurrentMap<String, ConsumerLog> logs;
    private final ConcurrentMap<String, Long> offsets;

    ConsumerLogManager(final StorageConfig config, final Map<String, Long> maxSequences) {
        this.config = config;
        this.logs = new ConcurrentHashMap<>();
        this.offsets = new ConcurrentHashMap<>();

        loadConsumerLogs(maxSequences);
    }

    private void loadConsumerLogs(final Map<String, Long> maxSequences) {
        LOG.info("Start load consumer logs");

        final File root = new File(config.getConsumerLogStorePath());
        final File[] consumerLogDirs = root.listFiles();
        if (consumerLogDirs != null) {
            for (final File consumerLogDir : consumerLogDirs) {
                if (!consumerLogDir.isDirectory()) {
                    continue;
                }

				final String subject = consumerLogDir.getName();
				if (BREAKING_WHITESPACE.matchesAnyOf(subject)) {
					LOG.error("consumer log directory name is invalid, skip. name: {}", subject);
					continue;
				}
                final Long maxSequence = maxSequences.get(subject);
                if (maxSequence == null) {
                    LOG.warn("cannot find max sequence for subject {} in checkpoint.", subject);
                    logs.put(subject, new ConsumerLog(config, subject));
                } else {
                    logs.put(subject, new ConsumerLog(config, subject, maxSequence));
                }
            }
        }

        LOG.info("Load consumer logs done");
    }

    void initConsumerLogOffset(final MessageMemTable table) {
        for (Map.Entry<String, ConsumerLog> entry : logs.entrySet()) {
            offsets.put(entry.getKey(), entry.getValue().nextSequence());
        }

        if (table == null) {
            return;
        }
        table.getNextSequences().forEach(offsets::put);
    }

    Map<String, Long> currentConsumerLogOffset() {
        final Map<String, Long> map = new HashMap<>();
        for (Map.Entry<String, ConsumerLog> entry : logs.entrySet()) {
            map.put(entry.getKey(), entry.getValue().nextSequence() - 1);
        }
        return map;
    }

    ConsumerLog getOrCreateConsumerLog(final String subject) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(subject), "message subject cannot be null or empty");
        if (!logs.containsKey(subject)) {
            synchronized (logs) {
                if (!logs.containsKey(subject)) {
                    logs.put(subject, new ConsumerLog(config, subject));
                }
            }
        }

        return logs.get(subject);
    }

    ConsumerLog getConsumerLog(final String subject) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(subject), "message subject cannot be null or empty");
        return logs.get(subject);
    }

    long getOffsetOrDefault(final String subject, final long defaultVal) {
        return offsets.getOrDefault(subject, defaultVal);
    }

    long incOffset(final String subject) {
        return offsets.compute(subject, (key, offset) -> offset == null ? 1 : offset + 1);
    }

    public void flush() {
        final long start = System.currentTimeMillis();
        try {
            for (final ConsumerLog log : logs.values()) {
                log.flush();
            }
        } finally {
            QMon.flushConsumerLogTimer(System.currentTimeMillis() - start);
        }
    }

    public void clean() {
        for (final ConsumerLog log : logs.values()) {
            log.clean();
        }
    }

    void adjustConsumerLogMinOffset(LogSegment firstSegment) {
        adjustConsumerLogMinOffset(config.getMessageLogStorePath(), firstSegment);
    }

    void adjustConsumerLogMinOffsetForSMT(final LogSegment firstSegment) {
        adjustConsumerLogMinOffset(config.getSMTStorePath(), firstSegment);
    }

    private void adjustConsumerLogMinOffset(final String path, final LogSegment firstSegment) {
        if (firstSegment == null) return;

        final String fileName = StoreUtils.offsetFileNameForSegment(firstSegment);
        final CheckpointStore<Map<String, Long>> offsetStore = new CheckpointStore<>(path, fileName, new ConsumerLogMinOffsetSerde());
        final Map<String, Long> offsets = offsetStore.loadCheckpoint();
        if (offsets == null) return;

        LOG.info("adjust consumer log min offset with offset file {}", fileName);

        for (Map.Entry<String, Long> entry : offsets.entrySet()) {
            final ConsumerLog log = logs.get(entry.getKey());
            if (log == null) {
                LOG.warn("cannot find consumer log {} while adjust min offset.", entry.getKey());
            } else {
                long adjustedMinOffset = entry.getValue() + 1;
                log.setMinSequence(adjustedMinOffset);
            }
        }
    }

    void createOffsetFileFor(long baseOffset, Map<String, Long> offsets) {
        final String fileName = StoreUtils.offsetFileNameOf(baseOffset);
        final CheckpointStore<Map<String, Long>> offsetStore = new CheckpointStore<>(config.getMessageLogStorePath(), fileName, new ConsumerLogMinOffsetSerde());
        offsetStore.saveCheckpoint(offsets);
    }

    @Override
    public void close() {
        for (final ConsumerLog log : logs.values()) {
            log.close();
        }
    }

    private static class ConsumerLogMinOffsetSerde implements Serde<Map<String, Long>> {

        @Override
        public byte[] toBytes(Map<String, Long> value) {
            try {
                return MAPPER.writeValueAsBytes(value);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("serialize message log min offset failed.", e);
            }
        }

        @Override
        public Map<String, Long> fromBytes(byte[] data) {
            try {
                return MAPPER.readValue(data, new TypeReference<Map<String, Long>>() {
                });
            } catch (IOException e) {
                throw new RuntimeException("deserialize offset checkpoint failed.", e);
            }
        }
    }
}
