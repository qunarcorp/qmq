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

package qunar.tc.qmq.meta.loadbalance;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public class RandomLoadBalance<T> extends AbstractLoadBalance<T> {
    @Override
    List<T> doSelect(String subject, List<T> brokerGroups, int minNum) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final Set<T> resultSet = new HashSet<>(minNum);
        while (resultSet.size() < minNum) {
            final int randomIndex = random.nextInt(brokerGroups.size());
            resultSet.add(brokerGroups.get(randomIndex));
        }

        return new ArrayList<>(resultSet);
    }
}
