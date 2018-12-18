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

    private final ConcurrentMap<String, AtomicReference<T>> configMap = new ConcurrentHashMap<>();

    public void update(final String configName, final Map<String, String> config) {
        // update or add config
        for (Map.Entry<String, String> entry : config.entrySet()) {
            setString(entry.getKey(), entry.getValue());
            LOGGER.info("set pull config: {}. {}={}", configName, entry.getKey(), entry.getValue());
        }

        // no configed
        for (String key : configMap.keySet()) {
            if (!config.containsKey(key)) {
                AtomicReference<T> valueRef = configMap.get(key);
                if (valueRef != null) {
                    T oldValue = valueRef.get();
                    updateValueOnNoConfiged(key, valueRef);
                    T newValue = valueRef.get();
                    LOGGER.info("update no pull config: {}. key={}, oldValue={}, newValue={}", configName, key, oldValue, newValue);
                }
            }
        }
    }

    private void setString(String key, String value) {
        Optional<T> v = parse(key, value);
        if (v.isPresent()) {
            set(key, v.get());
        }
    }

    private void set(String key, T value) {
        AtomicReference<T> old = configMap.putIfAbsent(key, new AtomicReference<T>(value));
        if (old != null) {
            old.set(value);
        }
    }

    public AtomicReference<T> get(String key, T defaultValue) {
        AtomicReference<T> tmp = new AtomicReference<>(defaultValue);
        AtomicReference<T> old = configMap.putIfAbsent(key, tmp);
        return old != null ? old : tmp;
    }

    protected abstract Optional<T> parse(String key, String value);

    protected abstract void updateValueOnNoConfiged(String key, AtomicReference<T> valueRef);
}
