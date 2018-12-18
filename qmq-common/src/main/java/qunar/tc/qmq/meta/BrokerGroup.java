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

package qunar.tc.qmq.meta;

import java.util.List;
import java.util.Objects;

/**
 * @author yunfeng.yang
 * @since 2017/8/28
 */
public class BrokerGroup {
    private String groupName;
    private String master;
    private List<String> slaves;
    private long updateTime;
    private BrokerState brokerState;
    private String tag;
    private BrokerGroupKind kind;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public BrokerState getBrokerState() {
        return brokerState;
    }

    public void setBrokerState(BrokerState brokerState) {
        this.brokerState = brokerState;
    }

    public List<String> getSlaves() {
        return slaves;
    }

    public void setSlaves(List<String> slaves) {
        this.slaves = slaves;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public BrokerGroupKind getKind() {
        return kind;
    }

    public void setKind(final BrokerGroupKind kind) {
        this.kind = kind;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerGroup group = (BrokerGroup) o;
        return Objects.equals(groupName, group.groupName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(groupName);
    }

    @Override
    public String toString() {
        return "BrokerGroup{" +
                "groupName='" + groupName + '\'' +
                ", master='" + master + '\'' +
                ", slaves=" + slaves +
                ", updateTime=" + updateTime +
                ", brokerState=" + brokerState +
                ", tag='" + tag + '\'' +
                '}';
    }
}
