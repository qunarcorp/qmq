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

package qunar.tc.qmq.delay.store.log;

import org.joda.time.LocalDateTime;


/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-13 11:16
 */
public class ScheduleOffsetResolver {

    static {
        LocalDateTime.now();
    }

    public static int resolveSegment(long offset) {
        LocalDateTime localDateTime = new LocalDateTime(offset);
        return localDateTime.getYear() * 1000000
                + localDateTime.getMonthOfYear() * 10000
                + localDateTime.getDayOfMonth() * 100
                + localDateTime.getHourOfDay();
    }
}
