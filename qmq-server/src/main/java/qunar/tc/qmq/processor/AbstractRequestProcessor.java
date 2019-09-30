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

package qunar.tc.qmq.processor;

import qunar.tc.qmq.meta.BrokerRole;
import qunar.tc.qmq.configuration.BrokerConfig;
import qunar.tc.qmq.netty.NettyRequestProcessor;

/**
 * Created by zhaohui.yu
 * 6/19/18
 */
public abstract class AbstractRequestProcessor implements NettyRequestProcessor {

    @Override
    public boolean rejectRequest() {
        return BrokerConfig.getBrokerRole() == BrokerRole.SLAVE;
    }
}
