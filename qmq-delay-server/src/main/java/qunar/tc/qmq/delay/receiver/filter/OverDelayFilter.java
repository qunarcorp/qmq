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

import java.util.concurrent.TimeUnit;


/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-01 16:26
 */
public class OverDelayFilter implements Filter {
    public static final long TWO_YEAR_MILLIS = TimeUnit.DAYS.toMillis(365 * 2);

    @Override
    public void invoke(Invoker invoker, ReceivedDelayMessage message) {
        if (message.getScheduleTime() > (System.currentTimeMillis() + TWO_YEAR_MILLIS)) {
            throw new OverDelayException("messageOverDelay");
        }

        invoker.invoke(message);
    }

}
