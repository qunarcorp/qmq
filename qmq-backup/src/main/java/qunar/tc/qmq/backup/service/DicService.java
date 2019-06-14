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

package qunar.tc.qmq.backup.service;

/**
 * @author yiqun.fan create on 17-10-31.
 */
public interface DicService {
    String SIX_DIGIT_FORMAT_PATTERN = "%06d";

    String MAX_CONSUMER_GROUP_ID = "999999";

    String MIN_CONSUMER_GROUP_ID = "000000";

    String name2Id(String name);

}
