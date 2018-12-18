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

public class BrokerAcquireMetaRequestSerializer {
    public static void serialize(BrokerAcquireMetaRequest request, ByteBuf out) {
        PayloadHolderUtils.writeString(request.getHostname(), out);
        out.writeInt(request.getPort());
    }

    public static BrokerAcquireMetaRequest deSerialize(ByteBuf out) {
        BrokerAcquireMetaRequest request = new BrokerAcquireMetaRequest();
        request.setHostname(PayloadHolderUtils.readString(out));
        request.setPort(out.readInt());
        return request;
    }
}
