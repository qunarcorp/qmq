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

package qunar.tc.qmq.metainfoclient;

import qunar.tc.qmq.meta.MetaServerLocator;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;
import qunar.tc.qmq.protocol.consumer.MetaInfoResponse;

/**
 * @author yiqun.fan create on 17-9-1.
 */
interface MetaInfoClient {
    void sendRequest(MetaInfoRequest request);

    void registerResponseSubscriber(ResponseSubscriber receiver);

    void setMetaServerLocator(MetaServerLocator locator);

    interface ResponseSubscriber {
        void onResponse(MetaInfoResponse response);
    }
}
