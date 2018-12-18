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

package qunar.tc.qmq.configuration.local;

import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author keli.wang
 * @since 2018-11-27
 */
public class LocalDynamicConfigFactory implements DynamicConfigFactory {
    private final ConfigWatcher watcher = new ConfigWatcher();
    private final ConcurrentMap<String, LocalDynamicConfig> configs = new ConcurrentHashMap<>();

    @Override
    public DynamicConfig create(final String name, final boolean failOnNotExist) {
        if (configs.containsKey(name)) {
            return configs.get(name);
        }

        return doCreate(name, failOnNotExist);
    }

    private LocalDynamicConfig doCreate(final String name, final boolean failOnNotExist) {
        final LocalDynamicConfig prev = configs.putIfAbsent(name, new LocalDynamicConfig(name, failOnNotExist));
        final LocalDynamicConfig config = configs.get(name);
        if (prev == null) {
            watcher.addWatch(config);
            config.onConfigModified();
        }
        return config;
    }
}
