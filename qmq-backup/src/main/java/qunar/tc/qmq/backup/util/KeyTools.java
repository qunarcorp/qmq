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

package qunar.tc.qmq.backup.util;

import com.google.common.hash.Hashing;
import org.jboss.netty.util.CharsetUtil;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * key生成工具类
 *
 * @author kelly.li
 * @date 2014-04-03
 */
public class KeyTools {

    private static final String DATE_FORMAT_PATTERN = "yyMMddHHmmss";
    private static final long MAX_LONG = 999999999999L;
    private static final String CREATE_DATE_FORMAT_PATTERN = "%12d";
    private static final long ONE_MINUTE_IN_MILLS = TimeUnit.MINUTES.toMillis(1);
    private static final String SEQUENCE_PATTERN = "0000000000000000000";
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat(SEQUENCE_PATTERN);

    private static final ThreadLocal<SimpleDateFormat> DATE_FORMATTER = ThreadLocal.withInitial(() -> new SimpleDateFormat(DATE_FORMAT_PATTERN));

    public static String generateDateKey(Date key) {
        String formatText = DATE_FORMATTER.get().format(key);
        Long dateLong = MAX_LONG - Long.parseLong(formatText);
        formatText = String.format(CREATE_DATE_FORMAT_PATTERN, dateLong);
        return formatText;
    }

    public static String generateMD5Key(String key) {
        return Hashing.md5().hashString(key, CharsetUtil.UTF_8).toString();
    }

    public static String generateMD5Key16(String key) {
        return generateMD5Key(key).substring(8, 24);
    }

    public static String generateDecimalFormatKey19(long key) {
        return DECIMAL_FORMAT.format(key);
    }

    public static String generateEndDateKey2(Date key) {
        Date d = new Date(key.getTime() - ONE_MINUTE_IN_MILLS);
        return generateDateKey(d);
    }

}
