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

import com.google.common.util.concurrent.ListenableFuture;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.netty.exception.ClientSendException;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.util.concurrent.ExecutionException;

/**
 * @author yiqun.fan create on 17-9-1.
 */
public interface MetaInfoClient {

    void reportConsumerState(String subject, String consumerGroup, String clientId, OnOfflineState state) throws ClientSendException, MetaServerNotFoundException;

    boolean queryOrderedSubject(String subject) throws MetaServerNotFoundException, ClientSendException, ExecutionException, InterruptedException;

    ListenableFuture<MetaInfoResponse> sendMetaInfoRequest(MetaInfoRequest request);

    void registerResponseSubscriber(ResponseSubscriber receiver);

    interface ResponseSubscriber {
        void onResponse(MetaInfoResponse response);
    }
}
