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

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.utils.PayloadHolderUtils;

public class BrokerRegisterRequestSerializer {
    public static void serialize(BrokerRegisterRequest request, ByteBuf out) {
        out.writeInt(request.getRequestType());
        PayloadHolderUtils.writeString(request.getGroupName(), out);
        PayloadHolderUtils.writeString(request.getBrokerAddress(), out);
        out.writeInt(request.getBrokerRole());
        out.writeInt(request.getBrokerState());
    }

    public static BrokerRegisterRequest deSerialize(ByteBuf out) {
        BrokerRegisterRequest request = new BrokerRegisterRequest();
        request.setRequestType(out.readInt());
        request.setGroupName(PayloadHolderUtils.readString(out));
        request.setBrokerAddress(PayloadHolderUtils.readString(out));
        request.setBrokerRole(out.readInt());
        request.setBrokerState(out.readInt());
        return request;
    }
}
