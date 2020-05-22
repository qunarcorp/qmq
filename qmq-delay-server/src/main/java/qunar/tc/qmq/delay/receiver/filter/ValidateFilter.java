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

package qunar.tc.qmq.delay.receiver.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import qunar.tc.qmq.delay.base.ReceivedDelayMessage;
import qunar.tc.qmq.delay.receiver.Invoker;

/**
 * 验证过滤器
 *
 * @author kelly.li
 */
public class ValidateFilter implements Filter {
    @Override
    public void invoke(Invoker invoker, ReceivedDelayMessage message) {
        Preconditions.checkNotNull(message, "message not null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(message.getMessageId()), "message id should not be empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(message.getSubject()), "message subject should not be empty");

        invoker.invoke(message);
    }
}
