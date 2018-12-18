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

package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zhaohui.yu
 * 5/29/18
 */
class WeightLoadBalance {

    private static final int MIN_WEIGHT = 5;

    private static final int MAX_WEIGHT = 150;

    private static final int DEFAULT_WEIGHT = 100;

    private final Random random = new Random();

    private ConcurrentMap<BrokerGroupInfo, Integer> weights = new ConcurrentHashMap<>();

    BrokerGroupInfo select(BrokerClusterInfo cluster) {
        List<BrokerGroupInfo> groups = cluster.getGroups();
        if (groups == null || groups.isEmpty()) return null;

        int size = groups.size();
        int totalWeight = 0;
        boolean sameWeight = true;
        int lastWeight = -1;
        for (int i = 0; i < size; ++i) {
            BrokerGroupInfo group = groups.get(i);
            if (!group.isAvailable()) continue;
            Integer weight = weights.get(group);
            if (weight == null) {
                weights.putIfAbsent(group, DEFAULT_WEIGHT);
                weight = weights.get(group);
            }
            if (lastWeight != -1 && lastWeight != weight) {
                sameWeight = false;
            }
            lastWeight = weight;
            totalWeight += weight;
        }
        if (totalWeight == 0) return null;

        if (totalWeight > 0 && !sameWeight) {
            int offset = random.nextInt(totalWeight);
            for (int i = 0; i < size; ++i) {
                BrokerGroupInfo group = groups.get(i);
                if (!group.isAvailable()) continue;
                Integer weight = weights.get(group);
                offset -= weight;
                if (offset <= 0) {
                    return group;
                }
            }
        }

        int index = random.nextInt(size);
        return groups.get(index);
    }

    public void timeout(BrokerGroupInfo group) {
        update(group, 0.25, DEFAULT_WEIGHT);
    }

    void noMessage(BrokerGroupInfo group) {
        update(group, 0.75, DEFAULT_WEIGHT);
    }

    void fetchedMessages(BrokerGroupInfo group) {
        update(group, 1.25, DEFAULT_WEIGHT);
    }

    void fetchedEnoughMessages(BrokerGroupInfo group) {
        update(group, 1.5, MAX_WEIGHT);
    }

    private void update(BrokerGroupInfo group, double factor, int maxWeight) {
        Integer weight = weights.get(group);
        if (weight == null) {
            weights.putIfAbsent(group, DEFAULT_WEIGHT);
            weight = weights.get(group);
        }

        weight = Math.min(Math.max((int) (weight * factor), MIN_WEIGHT), maxWeight);
        weights.put(group, weight);
    }
}
