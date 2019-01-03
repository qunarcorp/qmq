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

    public static long resolveSegment(long offset, int scale) {
        LocalDateTime localDateTime = new LocalDateTime(offset);
        long year = year(localDateTime);
        long month = month(localDateTime);
        long day = day(localDateTime);
        long hour = hour(localDateTime);
        long minute = minute(localDateTime);
        minute = minute - (minute % scale);
        return year + month + day + hour + minute;
    }

    private static long year(final LocalDateTime localDateTime) {
        return localDateTime.getYear() * 100000000L;
    }

    private static long month(final LocalDateTime localDateTime) {
        return localDateTime.getMonthOfYear() * 1000000L;
    }

    private static long day(final LocalDateTime localDateTime) {
        return localDateTime.getDayOfMonth() * 10000L;
    }

    private static long hour(final LocalDateTime localDateTime) {
        return localDateTime.getHourOfDay() * 100L;
    }

    private static long minute(final LocalDateTime localDateTime) {
        return localDateTime.getMinuteOfHour();
    }
}
