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

package qunar.tc.qmq.consumer.register;

import qunar.tc.qmq.PullConsumer;
import qunar.tc.qmq.PullEntry;

import java.util.concurrent.Future;

/**
 * User: zhaohuiyu
 * Date: 6/5/13
 * Time: 10:59 AM
 */
public interface ConsumerRegister {

    Future<PullEntry> registerPullEntry(String subject, String consumerGroup, RegistParam param);

    Future<PullConsumer> registerPullConsumer(String subject, String consumerGroup, boolean isBroadcast, boolean isOrdered);

    void unregister(String subject, String consumerGroup);

    void setAutoOnline(boolean autoOnline);

    void destroy();
}
