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

package qunar.tc.qmq.metrics;

import com.google.common.base.Supplier;

import java.util.ServiceLoader;

public class Metrics {
    private static final String[] EMPTY = new String[0];

    private static final QmqMetricRegistry INSTANCE;

    static {
        ServiceLoader<QmqMetricRegistry> registries = ServiceLoader.load(QmqMetricRegistry.class);
        QmqMetricRegistry instance = null;
        for (QmqMetricRegistry registry : registries) {
            instance = registry;
            break;
        }
        if (instance == null) {
            instance = new MockRegistry();
        }

        INSTANCE = instance;
    }

    public static void gauge(String name, String[] tags, String[] values, Supplier<Double> supplier) {
        INSTANCE.newGauge(name, tags, values, supplier);
    }

    public static void gauge(String name, Supplier<Double> supplier) {
        INSTANCE.newGauge(name, EMPTY, EMPTY, supplier);
    }

    public static QmqCounter counter(String name, String[] tags, String[] values) {
        return INSTANCE.newCounter(name, tags, values);
    }

    public static QmqCounter counter(String name) {
        return INSTANCE.newCounter(name, EMPTY, EMPTY);
    }

    public static QmqMeter meter(String name, String[] tags, String[] values) {
        return INSTANCE.newMeter(name, tags, values);
    }

    public static QmqMeter meter(String name) {
        return INSTANCE.newMeter(name, EMPTY, EMPTY);
    }

    public static QmqTimer timer(String name, String[] tags, String[] values) {
        return INSTANCE.newTimer(name, tags, values);
    }

    public static QmqTimer timer(String name) {
        return INSTANCE.newTimer(name, EMPTY, EMPTY);
    }

    public static void remove(String name, String[] tags, String[] values) {
        INSTANCE.remove(name, tags, values);
    }
}