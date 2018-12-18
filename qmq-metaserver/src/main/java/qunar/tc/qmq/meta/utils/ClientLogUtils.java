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

package qunar.tc.qmq.meta.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author keli.wang
 * @since 2018/8/22
 */
public final class ClientLogUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ClientLogUtils.class);

    private static final String DEFAULT_SWITCH = "default";

    private static volatile Map<String, Boolean> logSwitchMap = Collections.emptyMap();
    private static volatile boolean defaultSwitch = true;

    static {
        final DynamicConfig config = DynamicConfigLoader.load("client_log_switch.properties", false);
        config.addListener(conf -> {
            final Map<String, Boolean> tmp = new HashMap<>();
            conf.asMap().forEach((key, value) -> tmp.put(key, Boolean.parseBoolean(value)));

            defaultSwitch = tmp.getOrDefault(DEFAULT_SWITCH, true);
            logSwitchMap = tmp;
        });
    }

    private ClientLogUtils() {
    }

    public static void log(final String subject, final String format, final Object... arguments) {
        if (logSwitchMap.getOrDefault(subject, false)) {
            LOG.info(format, arguments);
        } else if (defaultSwitch) {
            LOG.info(format, arguments);
        }
    }
}
