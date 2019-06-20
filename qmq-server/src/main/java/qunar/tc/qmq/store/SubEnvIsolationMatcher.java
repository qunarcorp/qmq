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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.consumer.SubEnvIsolationPullFilter;
import qunar.tc.qmq.store.buffer.Buffer;
import qunar.tc.qmq.utils.CharsetUtils;
import qunar.tc.qmq.utils.Flags;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author keli.wang
 * @since 2019-01-03
 */
class SubEnvIsolationMatcher {
    private static final Logger LOG = LoggerFactory.getLogger(SubEnvIsolationMatcher.class);

    private static final Joiner RULE_KEY_JOINER = Joiner.on('_');

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledExecutorService reloadExecutor;
    private volatile ImmutableMap<String, SubEnvIsolationRule> ruleMap = ImmutableMap.of();

    SubEnvIsolationMatcher(final String rulesUrl) {
        this.reloadExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("reload-env-match-rules"));
        startScheduleReload(rulesUrl);
    }

    private void startScheduleReload(final String rulesUrl) {
        reloadExecutor.scheduleAtFixedRate(() -> reloadWithRetry(rulesUrl, 3), 0, 30, TimeUnit.SECONDS);
    }

    private void reloadWithRetry(final String rulesUrl, final int retryCount) {
        for (int i = 0; i < retryCount; i++) {
            try {
                if (reloadRules(rulesUrl)) {
                    return;
                }
            } catch (Exception e) {
                LOG.error("unknown exception caught.", e);
            }
        }
        LOG.info("reload env match rules failed after retry {} times. url: {}", retryCount, rulesUrl);
    }

    private boolean reloadRules(final String rulesUrl) {
        try {
            final String data = request(rulesUrl);
            final List<SubEnvIsolationRule> rules;
            try {
                rules = objectMapper.readValue(data, new TypeReference<List<SubEnvIsolationRule>>() {
                });
            } catch (Exception e) {
                LOG.error("Failed to deserialize env isolation rules.", e);
                return false;
            }
            ruleMap = toMap(rules);
            return true;
        } catch (Exception e) {
            LOG.error("reload env match rules failed. url: {}", rulesUrl, e);
            return false;
        }
    }

    private String request(final String httpUrl) throws IOException {
        final URL url = new URL(httpUrl);
        final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
            final StringBuilder sb = new StringBuilder();
            while (true) {
                final String line = reader.readLine();
                if (line == null) {
                    break;
                }
                sb.append(line);
            }
            return sb.toString();
        } finally {
            conn.disconnect();
        }
    }

    private ImmutableMap<String, SubEnvIsolationRule> toMap(final List<SubEnvIsolationRule> rules) {
        final ImmutableMap.Builder<String, SubEnvIsolationRule> builder = ImmutableMap.builder();
        for (final SubEnvIsolationRule rule : rules) {
            final String key = RULE_KEY_JOINER.join(rule.getSubject(), rule.getGroup(), rule.getGroupEnv(), rule.getGroupSubEnv());
            builder.put(key, rule);
        }
        return builder.build();
    }

    boolean match(final SubEnvIsolationPullFilter filter, final PullRequest request, final Buffer result) {
        final ByteBuffer message = result.getBuffer();
        message.mark();
        try {
            skipUntilBody(message);
            return isEnvMatch(filter, request, readBody(message));
        } catch (Exception e) {
            LOG.error("check env match failed.", e);
            return false;
        } finally {
            message.reset();
        }
    }

    private void skipUntilBody(final ByteBuffer message) {
        byte flag = message.get();

        skip(message, 8 + 8);
        //subject
        skipString(message);
        //message id
        skipString(message);

        if (Flags.hasTags(flag)) {
            skipTags(message);
        }
    }

    private void skipTags(final ByteBuffer message) {
        final byte tagsSize = message.get();
        for (int i = 0; i < tagsSize; i++) {
            skipString(message);
        }
    }

    private void skipString(final ByteBuffer message) {
        final short len = message.getShort();
        skip(message, len);
    }

    private void skip(final ByteBuffer message, final int bytes) {
        message.position(message.position() + bytes);
    }

    private Map<String, String> readBody(final ByteBuffer message) {
        final int bodySize = message.getInt();
        final ByteBuffer bodyBuffer = message.slice();
        bodyBuffer.limit(bodySize);

        final Map<String, String> body = new HashMap<>();
        while (bodyBuffer.hasRemaining()) {
            final String key = readString(bodyBuffer);
            final String value = readString(bodyBuffer);
            body.put(key, value);
        }
        return body;
    }

    private String readString(final ByteBuffer buffer) {
        final short length = buffer.getShort();
        if (length < 0) {
            throw new RuntimeException("message data corrupt");
        }

        if (length == 0) {
            return "";
        }

        final byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, CharsetUtils.UTF8);
    }

    private boolean isEnvMatch(final SubEnvIsolationPullFilter filter, final PullRequest request, final Map<String, String> body) {
        final String key = buildRuleKey(filter, request);
        final SubEnvIsolationRule rule = ruleMap.get(key);
        if (rule == null) {
            return false;
        }

        final String env = body.get(BaseMessage.keys.qmq_env.name());
        final String subEnv = body.get(BaseMessage.keys.qmq_subEnv.name());
        if (Strings.isNullOrEmpty(env) || subEnv == null) {
            return false;
        }

        return Objects.equals(rule.getSubjectEnv(), env) && Objects.equals(rule.getSubjectSubEnv(), subEnv);
    }

    private String buildRuleKey(final SubEnvIsolationPullFilter filter, final PullRequest request) {
        if (request.isBroadcast()) {
            return RULE_KEY_JOINER.join(request.getSubject(), "", filter.getEnv(), filter.getSubEnv());
        }
        return RULE_KEY_JOINER.join(request.getSubject(), request.getGroup());
    }
}
