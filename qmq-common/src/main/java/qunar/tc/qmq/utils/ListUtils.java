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

import java.util.Arrays;
import java.util.List;

/**
 * @author: leix.xie
 * @date: 2018/10/29 16:12
 * @describe：
 */
public class ListUtils {
    public static boolean contains(List<byte[]> source, byte[] target) {
        for (byte[] bytes : source) {
            if (Arrays.equals(bytes, target)) {
                return true;
            }
        }
        return false;
    }

    public static boolean containsAll(List<byte[]> source, List<byte[]> target) {
        if (source.size() < target.size()) return false;
        for (byte[] bytes : target)
            if (!contains(source, bytes)) {
                return false;
            }
        return true;
    }

    public static boolean intersection(List<byte[]> source, List<byte[]> target) {
        for (byte[] bytes : target) {
            if (contains(source, bytes)) {
                return true;
            }
        }
        return false;
    }
}
