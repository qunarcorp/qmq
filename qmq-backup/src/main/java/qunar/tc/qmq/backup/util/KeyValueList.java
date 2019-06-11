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

import org.hbase.async.KeyValue;

import java.util.List;
import java.util.Map;

public interface KeyValueList<T> {

    String getStringValue(String qualifier);

    byte[] getValue(String qualifier);

    Long getTimestamp(String qualifier);

    byte[] getKey();

    Map<String, T> getData();

    int size();

    List<KeyValue> getKeyValues();
}
