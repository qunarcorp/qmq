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

package qunar.tc.qmq.metrics.prometheus;

import io.prometheus.client.Gauge;
import qunar.tc.qmq.metrics.QmqCounter;

/**
 * @author keli.wang
 * @since 2018/11/22
 */
public class PrometheusQmqCounter implements QmqCounter {
    private final Gauge.Child gauge;

    public PrometheusQmqCounter(final Gauge gauge, final String[] labels) {
        this.gauge = gauge.labels(labels);
    }

    @Override
    public void inc() {
        gauge.inc();
    }

    @Override
    public void inc(final long n) {
        gauge.inc(n);
    }

    @Override
    public void dec() {
        gauge.dec();
    }

    @Override
    public void dec(final long n) {
        gauge.dec(n);
    }
}
