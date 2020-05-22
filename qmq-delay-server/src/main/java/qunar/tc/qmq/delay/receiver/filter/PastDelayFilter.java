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

import qunar.tc.qmq.delay.base.ReceivedDelayMessage;
import qunar.tc.qmq.delay.receiver.Invoker;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-09-06 11:36
 */
public class PastDelayFilter implements Filter {
    @Override
    public void invoke(Invoker invoker, ReceivedDelayMessage message) {
        long now = System.currentTimeMillis();
        if (message.getScheduleTime() < now) {
            message.adjustScheduleTime(now);
        }

        invoker.invoke(message);
    }
}
