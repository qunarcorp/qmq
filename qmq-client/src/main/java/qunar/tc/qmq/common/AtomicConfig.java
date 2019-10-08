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

package qunar.tc.qmq.common;

import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public abstract class AtomicConfig<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtomicConfig.class);

    private static final String ANY = "*";

    private final ConcurrentMap<String, AtomicReference<T>> configMap = new ConcurrentHashMap<>();

    public void update(final String configName, final Map<String, String> config) {
        for (Map.Entry<String, String> entry : config.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            Optional<T> v = parse(key, value);
            if (v.isPresent()) {
                set(key, v.get());
            }
            LOGGER.info("set pull config: {}. {}={}", configName, key, value);
        }

        for (String key : configMap.keySet()) {
            if (!config.containsKey(key)) {
                reset(configName, key);
            }
        }
    }

    private void reset(String configName, String key) {
        AtomicReference<T> valueRef = configMap.get(key);
        if (valueRef != null) {
            T oldValue = valueRef.get();
            T defaultValue = getDefaultValue();
            valueRef.set(defaultValue);
            LOGGER.info("update no pull config: {}. key={}, oldValue={}, newValue={}", configName, key, oldValue, defaultValue);
        }
    }

    private void set(String key, T value) {
        AtomicReference<T> old = configMap.putIfAbsent(key, new AtomicReference<T>(value));
        if (old != null) {
            old.set(value);
        }
    }

    public AtomicReference<T> get(String key, T defaultValue) {
        AtomicReference<T> valueRef = configMap.get(key);
        if (valueRef != null) return valueRef;

        valueRef = configMap.get(ANY);
        if (valueRef != null) return valueRef;

        valueRef = new AtomicReference<>(defaultValue);
        AtomicReference<T> old = configMap.putIfAbsent(key, valueRef);
        return old != null ? old : valueRef;
    }

    protected abstract Optional<T> parse(String key, String value);

    protected abstract T getDefaultValue();
}
