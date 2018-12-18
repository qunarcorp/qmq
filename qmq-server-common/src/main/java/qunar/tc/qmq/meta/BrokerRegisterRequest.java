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

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
public class BrokerRegisterRequest {
    private String groupName;
    private int brokerRole;
    private int brokerState;
    private int requestType;
    private String brokerAddress;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public int getRequestType() {
        return requestType;
    }

    public void setRequestType(int requestType) {
        this.requestType = requestType;
    }

    public int getBrokerRole() {
        return brokerRole;
    }

    public void setBrokerRole(int brokerRole) {
        this.brokerRole = brokerRole;
    }

    public int getBrokerState() {
        return brokerState;
    }

    public void setBrokerState(int brokerState) {
        this.brokerState = brokerState;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    @Override
    public String toString() {
        return "BrokerRegisterRequest{" +
                "groupName='" + groupName + '\'' +
                ", brokerRole=" + brokerRole +
                ", brokerState=" + brokerState +
                ", requestType=" + requestType +
                ", brokerAddress='" + brokerAddress + '\'' +
                '}';
    }
}
