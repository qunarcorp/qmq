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

package qunar.tc.qmq.delay.store.visitor;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Optional;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-15 15:01
 */
public class DispatchLogVisitor extends AbstractLogVisitor<Long> {
    private static final int WORKING_SIZE = 1024 * 1024 * 10;

    public DispatchLogVisitor(long from, FileChannel fileChannel) {
        super(from, fileChannel, WORKING_SIZE);
    }

    @Override
    protected Optional<Long> readOneRecord(ByteBuffer buffer) {
        if (buffer.remaining() < 8) {
            return Optional.empty();
        }
        int startPos = buffer.position();
        long index = buffer.getLong();

        buffer.position(startPos + 8);
        return Optional.of(index);
    }
}
