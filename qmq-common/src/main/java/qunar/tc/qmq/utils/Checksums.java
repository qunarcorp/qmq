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

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

public final class Checksums {

    private Checksums() {
    }

    public static void update(Checksum checksum, ByteBuffer buffer, int length) {
        update(checksum, buffer, 0, length);
    }

    public static void update(Checksum checksum, ByteBuffer buffer, int offset, int length) {
        if (buffer.hasArray()) {
            checksum.update(buffer.array(), buffer.position() + buffer.arrayOffset() + offset, length);
        } else {
            int start = buffer.position() + offset;
            for (int i = start; i < start + length; i++)
                checksum.update(buffer.get(i));
        }
    }
}
