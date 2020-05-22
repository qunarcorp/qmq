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

package qunar.tc.qmq.utils;

import qunar.tc.qmq.Message;

import java.util.Date;

/**
 * Created by zhaohui.yu
 * 6/14/18
 */
public class DelayUtil {
    private static final long MIN_DELAY_TIME = 500;

    public static boolean isDelayMessage(Message message) {
        Date receiveTime = message.getScheduleReceiveTime();
        return receiveTime != null && (receiveTime.getTime() - System.currentTimeMillis()) >= MIN_DELAY_TIME;
    }
}
