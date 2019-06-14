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

/**
 * Created by zhaohui.yu
 * 8/22/18
 */
public class Flags {
    public static byte setDelay(byte flag, boolean isDelay) {
        return !isDelay ? flag : (byte) (flag | 2);
    }

    public static byte setReliability(byte flag, boolean isReliability) {
        return isReliability ? flag : (byte) (flag | 1);
    }

    public static byte setTags(byte flag, boolean hasTag) {
        return hasTag ? (byte) (flag | 4) : flag;
    }

    public static boolean isDelay(byte flag) {
        return (flag & 2) == 2;
    }

    public static boolean isReliability(byte flag) {
        return (flag & 1) != 1;
    }

    public static boolean hasTags(byte flag) {
        return (flag & 4) == 4;
    }
}
