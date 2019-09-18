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

package qunar.tc.qmq.protocol;

/**
 * @author yunfeng.yang
 * @since 2017/6/30
 */
public interface CommandCode {
    short PLACEHOLDER = -1;

    // response code
    short SUCCESS = 0;
    short UNKNOWN_CODE = 3;
    short NO_MESSAGE = 4;

    short BROKER_ERROR = 51;
    short BROKER_REJECT = 52;
    short PARAM_ERROR = 53;

    // request code
    short SEND_MESSAGE = 10;
    short PULL_MESSAGE = 11;
    short ACK_REQUEST = 12;

    short SYNC_LOG_REQUEST = 20;
    short SYNC_CHECKPOINT_REQUEST = 21;

    short QUERY_CONSUMER_LAG = 24;
    short CONSUME_MANAGE = 25;
    short QUEUE_COUNT = 26;
    short ACTION_OFFSET_REQUEST = 27;

    short BROKER_REGISTER = 30;
    short CLIENT_REGISTER = 35;

    short BROKER_ACQUIRE_META = 40;

    short UID_ASSIGN = 50;
    short UID_ACQUIRE = 51;

    // partitions
    short QUERY_SUBJECT = 60;
    short RELEASE_PULL_LOCK = 61;

    // heartbeat
    short HEARTBEAT = 100;

}
