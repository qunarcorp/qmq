/*
 * Copyright 2018 Qunar
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
 * limitations under the License.com.qunar.pay.trade.api.card.service.usercard.UserCardQueryFacade
 */

package qunar.tc.qmq.metrics.prometheus;

import com.google.common.base.Supplier;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import io.prometheus.client.SimpleCollector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PrometheusQmqGauge extends SimpleCollector<PrometheusQmqGauge.Child> implements Collector.Describable {

    PrometheusQmqGauge(Builder b) {
        super(b);
    }

    public static Builder build() {
        return new Builder();
    }

    @Override
    protected Child newChild() {
        return new Child();
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples.Sample> samples = new ArrayList<>(children.size());
        for (Map.Entry<List<String>, Child> c : children.entrySet()) {
            samples.add(new MetricFamilySamples.Sample(fullname, labelNames, c.getKey(), c.getValue().get()));
        }
        return familySamplesList(Type.GAUGE, samples);
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return Collections.singletonList(new GaugeMetricFamily(fullname, help, labelNames));
    }

    public static class Builder extends SimpleCollector.Builder<Builder, PrometheusQmqGauge> {
        @Override
        public PrometheusQmqGauge create() {
            return new PrometheusQmqGauge(this);
        }
    }

    public static class Child {
        private Supplier<Double> supplier;

        public void setSupplier(final Supplier<Double> supplier) {
            this.supplier = supplier;
        }

        public double get() {
            if (supplier == null) {
                return 0;
            } else {
                return supplier.get();
            }
        }
    }
}

