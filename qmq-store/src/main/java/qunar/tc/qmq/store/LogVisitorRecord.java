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

package qunar.tc.qmq.store;

/**
 * @author keli.wang
 * @since 2019-01-15
 */
public class LogVisitorRecord<T> {
    private final RecordType type;
    private final T data;

    private LogVisitorRecord(final RecordType type, final T data) {
        this.type = type;
        this.data = data;
    }

    public static <T> LogVisitorRecord<T> noMore() {
        return new LogVisitorRecord<>(RecordType.NO_MORE, null);
    }

    public static <T> LogVisitorRecord<T> empty() {
        return new LogVisitorRecord<>(RecordType.EMPTY, null);
    }

    public static <T> LogVisitorRecord<T> data(final T data) {
        return new LogVisitorRecord<>(RecordType.DATA, data);
    }

    public boolean isNoMore() {
        return type == RecordType.NO_MORE;
    }

    public boolean hasData() {
        return type == RecordType.DATA;
    }

    public T getData() {
        return data;
    }

    public enum RecordType {
        NO_MORE, EMPTY, DATA
    }
}
