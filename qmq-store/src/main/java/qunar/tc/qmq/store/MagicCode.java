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
 * @since 2017/7/6
 */
public final class MagicCode {
    public static final short CONSUMER_LOG_MAGIC_V2 = (short) 0xA3B2;

    public static final int MESSAGE_LOG_MAGIC_V1 = 0xA1B2C300;

    // add crc field for message body
    public static final int MESSAGE_LOG_MAGIC_V2 = 0xA1B2C301;

    //add tags field for message header
    public static final int MESSAGE_LOG_MAGIC_V3 = 0xA1B2C302;

    public static final int PULL_LOG_MAGIC_V1 = 0xC1B2A300;

    public static final int ACTION_LOG_MAGIC_V1 = 0xC3B2A100;

    public static final int SORTED_MESSAGES_TABLE_MAGIC_V1 = 0xA2B100;
}
