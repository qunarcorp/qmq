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

import com.google.common.base.Strings;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

/**
 * @author yiqun.fan create on 17-7-6.
 */
public class CharsetUtils {
    public static final Charset UTF8 = Charset.forName("utf-8");

    private static final byte[] EMPTY_BYTES = new byte[0];

    public static byte[] toUTF8Bytes(final String s) {
        try {
            return Strings.isNullOrEmpty(s) ? EMPTY_BYTES : s.getBytes("utf-8");
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    public static String toUTF8String(final byte[] bs) {
        try {
            return bs == null || bs.length == 0 ? "" : new String(bs, "utf-8");
        } catch (UnsupportedEncodingException e) {
            return "";
        }
    }
}
