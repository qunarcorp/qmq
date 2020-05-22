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

package qunar.tc.qmq.store.action;

import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.ActionReaderWriter;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2018/8/20
 */
public class ForeverOfflineActionReaderWriter implements ActionReaderWriter {
    @Override
    public int write(ByteBuffer to, Action action) {
        final int startIndex = to.position();

        PayloadHolderUtils.writeString(action.subject(), to);
        PayloadHolderUtils.writeString(action.group(), to);
        PayloadHolderUtils.writeString(action.consumerId(), to);
        to.putLong(action.timestamp());

        return to.position() - startIndex;
    }

    @Override
    public Action read(ByteBuffer from) {
        final String subject = PayloadHolderUtils.readString(from);
        final String group = PayloadHolderUtils.readString(from);
        final String consumerId = PayloadHolderUtils.readString(from);

        final long timestamp = from.getLong();
        return new ForeverOfflineAction(subject, group, consumerId, timestamp);
    }
}
