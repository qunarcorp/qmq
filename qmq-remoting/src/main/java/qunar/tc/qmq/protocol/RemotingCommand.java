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

package qunar.tc.qmq.protocol;

/**
 * @author yunfeng.yang
 * @since 2017/7/3
 */
public class RemotingCommand extends Datagram {
    private long receiveTime;

    public RemotingCommandType getCommandType() {
        int bits = 1;
        int flag0 = this.header.getFlag() & bits;
        return RemotingCommandType.codeOf(flag0);
    }

    public boolean isOneWay() {
        int bits = 1 << 1;
        return (this.header.getFlag() & bits) == bits;
    }

    public int getOpaque() {
        return header.getOpaque();
    }

    public long getReceiveTime() {
        return receiveTime;
    }

    public void setReceiveTime(long receiveTime) {
        this.receiveTime = receiveTime;
    }
}
