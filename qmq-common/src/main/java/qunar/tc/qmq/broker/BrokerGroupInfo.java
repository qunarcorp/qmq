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

package qunar.tc.qmq.broker;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.List;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class BrokerGroupInfo {

    private final int groupIndex;
    private final String groupName;
    private final String master;
    private final List<String> slaves;
    private volatile boolean available = true;

    private final CircuitBreaker circuitBreaker;

    public BrokerGroupInfo(int groupIndex, String groupName, String master, List<String> slaves) {
        this(groupIndex, groupName, master, slaves, new CircuitBreaker());
    }

    public BrokerGroupInfo(int groupIndex, String groupName, String master, List<String> slaves, CircuitBreaker circuitBreaker) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(groupName), "groupName不能是空");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(master), "master不能是空");
        this.groupIndex = groupIndex;
        this.groupName = groupName;
        this.master = master;
        this.slaves = slaves;
        this.circuitBreaker = circuitBreaker;
    }

    public int getGroupIndex() {
        return groupIndex;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getMaster() {
        return master;
    }

    public List<String> getSlaves() {
        return slaves;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    public boolean isAvailable() {
        return circuitBreaker.isAvailable() && available;
    }

    public void markFailed() {
        circuitBreaker.markFailed();
    }

    public void markSuccess() {
        circuitBreaker.markSuccess();
    }

    public static boolean isInvalid(BrokerGroupInfo brokerGroup) {
        return brokerGroup == null || !brokerGroup.isAvailable();
    }

    public CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    @Override
    public String toString() {
        return "BrokerGroupInfo{group=" + groupName + ", "
                + "master=" + master + ", "
                + "slaves=" + slaves + "}";
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || (obj instanceof BrokerGroupInfo && groupName.equals(((BrokerGroupInfo) obj).groupName));
    }

    @Override
    public int hashCode() {
        return groupName.hashCode();
    }
}
