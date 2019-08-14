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

package qunar.tc.qmq.common;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yiqun.fan create on 17-8-18.
 */
public class MapKeyBuilder {
    private static final String SEPARATOR = "$";

    private static final Splitter SPLITTER = Splitter.on(SEPARATOR).trimResults().omitEmptyStrings();

    public static String buildSubscribeKey(String subject, String group) {
        return Strings.nullToEmpty(subject) + SEPARATOR + Strings.nullToEmpty(group);
    }

    public static String buildSenderKey(String brokerGroupName, String subject, String group) {
        return brokerGroupName + MapKeyBuilder.SEPARATOR + buildSubscribeKey(subject, group);
    }

    public static String buildMetaInfoKey(ClientType clientType, String subject) {
        return clientType.name() + MapKeyBuilder.SEPARATOR + subject;
    }

    public static List<String> splitKey(String key) {
        return Strings.isNullOrEmpty(key) ? new ArrayList<String>() : SPLITTER.splitToList(key);
    }
}
