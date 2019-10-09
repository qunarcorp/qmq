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

package qunar.tc.qmq.config;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.AtomicConfig;
import qunar.tc.qmq.common.AtomicIntegerConfig;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.configuration.Listener;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yiqun.fan create on 17-8-17.
 */
public class PullSubjectsConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(PullSubjectsConfig.class);

    private static final String PULL_SUBJECTS_CONFIG = "pull_subject_config.properties";

    private static final PullSubjectsConfig config = new PullSubjectsConfig();
    private final Map<ConfigType, AtomicConfig> configMap;
    private final AtomicIntegerConfig pullBatchSizeConfig;
    private final AtomicIntegerConfig pullTimeoutConfig;
    private final AtomicIntegerConfig pullRequestTimeoutConfig;
    private final AtomicIntegerConfig ackNosendLimit;
    private final AtomicIntegerConfig maxRetryNum;

    private PullSubjectsConfig() {
        pullBatchSizeConfig = new AtomicIntegerConfig(50, 1, 10000);
        pullTimeoutConfig = new AtomicIntegerConfig(1000, -1, Integer.MAX_VALUE);
        pullRequestTimeoutConfig = new AtomicIntegerConfig(8000, 5000, Integer.MAX_VALUE);
        ackNosendLimit = new AtomicIntegerConfig(100, Integer.MIN_VALUE, Integer.MAX_VALUE);
        maxRetryNum = new AtomicIntegerConfig(32, 0, Integer.MAX_VALUE);

        configMap = new HashMap<>();
        configMap.put(ConfigType.PULL_BATCHSIZE, pullBatchSizeConfig);
        configMap.put(ConfigType.PULL_TIMEOUT, pullTimeoutConfig);
        configMap.put(ConfigType.PULL_REQUEST_TIMEOUT, pullRequestTimeoutConfig);
        configMap.put(ConfigType.ACK_NOSEND_LIMIT, ackNosendLimit);
        configMap.put(ConfigType.MAX_RETRY_NUM, maxRetryNum);
        loadConfig();
    }

    public static PullSubjectsConfig get() {
        return config;
    }

    private void loadConfig() {
        final DynamicConfig config = DynamicConfigLoader.load(PULL_SUBJECTS_CONFIG, false);
        config.addListener(new Listener() {
            @Override
            public void onLoad(final DynamicConfig config) {
                Map<ConfigType, Map<String, String>> subjectConfigMap = new HashMap<>();
                for (ConfigType configType : ConfigType.values()) {
                    subjectConfigMap.put(configType, new HashMap<String, String>());
                }

                final Map<String, String> originConf = config.asMap();
                for (Map.Entry<String, String> e : originConf.entrySet()) {
                    if (Strings.isNullOrEmpty(e.getKey()) || Strings.isNullOrEmpty(e.getValue())) continue;

                    String key = e.getKey();
                    for (ConfigType configType : ConfigType.values()) {
                        if (key.endsWith(configType.suffix)) {
                            String subject = key.substring(0, key.length() - configType.suffix.length());
                            if (Strings.isNullOrEmpty(subject)) {
                                LOGGER.warn("can't parse subject, please check config. {}={}", key, e.getValue());
                                break;
                            }
                            Map<String, String> subjectConfig = subjectConfigMap.get(configType);
                            subjectConfig.put(subject, e.getValue());
                            break;
                        }
                    }
                }

                for (Map.Entry<ConfigType, Map<String, String>> e : subjectConfigMap.entrySet()) {
                    AtomicConfig atomicConfig = configMap.get(e.getKey());
                    if (atomicConfig == null) {
                        continue;
                    }
                    atomicConfig.update(e.getKey().name(), e.getValue());
                }

            }
        });
    }

    public AtomicReference<Integer> getPullBatchSize(String subject) {
        return pullBatchSizeConfig.get(subject);
    }

    public AtomicReference<Integer> getPullTimeout(String subject) {
        return pullTimeoutConfig.get(subject);
    }

    public AtomicReference<Integer> getPullRequestTimeout(String subject) {
        return pullRequestTimeoutConfig.get(subject);
    }

    public AtomicReference<Integer> getAckNosendLimit(String subject) {
        return ackNosendLimit.get(subject);
    }

    public AtomicReference<Integer> getMaxRetryNum(String subject) {
        return maxRetryNum.get(subject);
    }

    public enum ConfigType {
        PULL_BATCHSIZE("_pullBatchSize"),
        PULL_TIMEOUT("_pullTimeout"),
        PULL_REQUEST_TIMEOUT("_pullRequestTimeout"),
        ACK_TIMEOUT("_ackTimeout"),
        ACK_NOSEND_LIMIT("_ackNosendLimit"),
        MAX_RETRY_NUM("_maxRetryNum");

        private final String suffix;

        ConfigType(String suffix) {
            this.suffix = suffix;
        }
    }
}
