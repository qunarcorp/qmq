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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.io.LineReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author keli.wang
 * @since 2018/9/10
 *
 * format:
 *
 * format version
 * action log offset
 * (
 *  subject/consumer group size
 *  (
 *      consumer group/is exclusive consume/pull offset/consumers size
 *          (consumer id/pull offset/ack offset)*
 *  )*
 * )*
 */
public class ActionCheckpointSerde implements Serde<ActionCheckpoint> {
    private static final int VERSION_V1 = 1;
    private static final int VERSION_V2 = 2;
    private static final int VERSION_V3 = 3;

    private static final char NEWLINE = '\n';
    private static final Joiner SLASH_JOINER = Joiner.on('/');
    private static final Splitter SLASH_SPLITTER = Splitter.on('/');

    @Override
    public byte[] toBytes(final ActionCheckpoint state) {
        final StringBuilder data = new StringBuilder();
        data.append(VERSION_V3).append(NEWLINE);
        data.append(state.getOffset()).append(NEWLINE);

        final Table<String, String, ConsumerGroupProgress> progresses = state.getProgresses();
        for (final String subject : progresses.rowKeySet()) {
            final Map<String, ConsumerGroupProgress> groups = progresses.row(subject);
            data.append(SLASH_JOINER.join(subject, groups.size())).append(NEWLINE);

            for (final String group : groups.keySet()) {
                final ConsumerGroupProgress progress = groups.get(group);
                final Map<String, ConsumerProgress> consumers = progress.getConsumers();
                final int consumerCount = consumers == null ? 0 : consumers.size();

                data.append(SLASH_JOINER.join(group, boolean2Short(progress.isExclusiveConsume()), progress.getPull(), consumerCount)).append(NEWLINE);

                if (consumerCount <= 0) {
                    continue;
                }

                consumers.values().forEach(consumer -> {
                    data.append(SLASH_JOINER.join(consumer.getConsumerId(), consumer.getPull(), consumer.getAck())).append(NEWLINE);
                });
            }
        }
        return data.toString().getBytes(Charsets.UTF_8);
    }

    @Override
    public ActionCheckpoint fromBytes(final byte[] data) {

        try {
            final LineReader reader = new LineReader(new StringReader(new String(data, Charsets.UTF_8)));
            final int version = Integer.parseInt(reader.readLine());
            switch (version) {
                case VERSION_V1:
                    throw new RuntimeException("v1 checkpoint not support");
                case VERSION_V2:
                    throw new RuntimeException("v2 checkpoint not support");
                case VERSION_V3:
                    return parseV3(reader);
                default:
                    throw new RuntimeException("unknown snapshot content version " + version);

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private ActionCheckpoint parseV3(LineReader reader) throws IOException {
        final long offset = Long.parseLong(reader.readLine());

        final Table<String, String, ConsumerGroupProgress> progresses = HashBasedTable.create();
        while (true) {
            final String subjectLine = reader.readLine();
            if (Strings.isNullOrEmpty(subjectLine)) {
                break;
            }

            final List<String> subjectParts = SLASH_SPLITTER.splitToList(subjectLine);
            final String subject = subjectParts.get(0);
            final int groupCount = Integer.parseInt(subjectParts.get(1));
            for (int i = 0; i < groupCount; i++) {
                final String groupLine = reader.readLine();
                final List<String> groupParts = SLASH_SPLITTER.splitToList(groupLine);
                final String group = groupParts.get(0);
                final boolean exclusiveConsume = short2Boolean(Short.parseShort(groupParts.get(1)));
                final long maxPulledMessageSequence = Long.parseLong(groupParts.get(2));
                final int consumerCount = Integer.parseInt(groupParts.get(3));

                final ConsumerGroupProgress progress = new ConsumerGroupProgress(subject, group, exclusiveConsume, maxPulledMessageSequence, new HashMap<>(consumerCount));
                progresses.put(subject, group, progress);

                final Map<String, ConsumerProgress> consumers = progress.getConsumers();
                for (int j = 0; j < consumerCount; j++) {
                    final String consumerLine = reader.readLine();
                    final List<String> consumerParts = SLASH_SPLITTER.splitToList(consumerLine);
                    final String consumerId = consumerParts.get(0);
                    final long pull = Long.parseLong(consumerParts.get(1));
                    final long ack = Long.parseLong(consumerParts.get(2));

                    consumers.put(consumerId, new ConsumerProgress(subject, group, consumerId, pull, ack));
                }
            }
        }

        return new ActionCheckpoint(offset, progresses);
    }

    private short boolean2Short(final boolean bool) {
        if (bool) {
            return 1;
        } else {
            return 0;
        }
    }

    private boolean short2Boolean(final short n) {
        return n != 0;
    }
}
