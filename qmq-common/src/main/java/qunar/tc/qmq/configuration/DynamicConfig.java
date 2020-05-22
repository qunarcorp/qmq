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

package qunar.tc.qmq.configuration;

import java.util.Map;

/**
 * User: zhaohuiyu Date: 12/24/12 Time: 4:12 PM
 */
public interface DynamicConfig {
    void addListener(Listener listener);

    String getString(String name);

    String getString(String name, String defaultValue);

    int getInt(String name);

    int getInt(String name, int defaultValue);

    long getLong(String name);

    long getLong(String name, long defaultValue);

    double getDouble(String name);

    double getDouble(String name, double defaultValue);

    boolean getBoolean(String name, boolean defaultValue);

    boolean exist(String name);

    Map<String, String> asMap();
}
