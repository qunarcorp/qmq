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

import java.util.List;

/**
 * User: zhaohuiyu Date: 1/9/13 Time: 10:35 AM
 */
public abstract class AbstractLoadBalance<T> implements LoadBalance<T> {
    @Override
    public List<T> select(String subject, List<T> brokerGroups, int minNum) {
        if (brokerGroups == null || brokerGroups.size() == 0) {
            return null;
        }
        if (brokerGroups.size() <= minNum) {
            return brokerGroups;
        }
        return doSelect(subject, brokerGroups, minNum);
    }

    abstract List<T> doSelect(String subject, List<T> brokerGroups, int minNum);
}
