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
import com.google.common.io.LineReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author keli.wang
 * @since 2018/9/12
 */
public class MessageCheckpointSerde implements Serde<MessageCheckpoint> {
    private static final int VERSION_V1 = 1;
    private static final int VERSION_V2 = 2;

    private static final char NEWLINE = '\n';
    private static final Joiner SLASH_JOINER = Joiner.on('/');
    private static final Splitter SLASH_SPLITTER = Splitter.on('/');

    @Override
    public byte[] toBytes(final MessageCheckpoint state) {
        final StringBuilder data = new StringBuilder();
        data.append(VERSION_V2).append(NEWLINE);
        data.append(state.getOffset()).append(NEWLINE);
        state.getMaxSequences().forEach((subject, sequence) -> data.append(SLASH_JOINER.join(subject, sequence)).append(NEWLINE));
        return data.toString().getBytes(Charsets.UTF_8);
    }

    @Override
    public MessageCheckpoint fromBytes(final byte[] data) {
        try {
            final LineReader reader = new LineReader(new StringReader(new String(data, Charsets.UTF_8)));
            final int version = Integer.parseInt(reader.readLine());
            switch (version) {
                case VERSION_V1:
                    throw new RuntimeException("v1 checkpoint not support");
                case VERSION_V2:
                    return parseV2(reader);
                default:
                    throw new RuntimeException("unknown snapshot content version " + version);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private MessageCheckpoint parseV2(final LineReader reader) throws IOException {
        final long offset = Long.parseLong(reader.readLine());
        final Map<String, Long> sequences = new HashMap<>();
        while (true) {
            final String line = reader.readLine();
            if (Strings.isNullOrEmpty(line)) {
                break;
            }

            final List<String> parts = SLASH_SPLITTER.splitToList(line);
            final String subject = parts.get(0);
            final long maxSequence = Long.parseLong(parts.get(1));
            sequences.put(subject, maxSequence);
        }

        return new MessageCheckpoint(offset, sequences);
    }
}
