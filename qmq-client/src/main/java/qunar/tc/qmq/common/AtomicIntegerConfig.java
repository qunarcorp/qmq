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

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yiqun.fan create on 17-8-17.
 */
public class AtomicIntegerConfig extends AtomicConfig<Integer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtomicIntegerConfig.class);

    private final int defaultValue;
    private final int minValue;
    private final int maxValue;

    public AtomicIntegerConfig(int defaultValue, int minValue, int maxValue) {
        this.defaultValue = defaultValue;
        this.minValue = Math.min(minValue, maxValue);
        this.maxValue = Math.max(minValue, maxValue);
    }

    public AtomicReference<Integer> get(String key) {
        return get(key, defaultValue);
    }

    @Override
    public Optional<Integer> parse(String key, String value) {
        int v = defaultValue;
        try {
            v = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOGGER.warn("parse config fail: {}={}, use default: {}", key, value, v);
        }
        if (v < minValue || v > maxValue) {
            LOGGER.warn("config value {} out of range: [{}, {}], use default: {}",
                    v, minValue, maxValue, defaultValue);
            v = defaultValue;
        }
        return Optional.of(v);
    }

    @Override
    protected Integer getDefaultValue() {
        return defaultValue;
    }
}
