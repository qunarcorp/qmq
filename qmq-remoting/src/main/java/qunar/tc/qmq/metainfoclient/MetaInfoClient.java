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

import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.QuerySubjectRequest;
import qunar.tc.qmq.protocol.QuerySubjectResponse;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @author yiqun.fan create on 17-9-1.
 */
public interface MetaInfoClient {

    void sendMetaInfoRequest(MetaInfoRequest request);

    /**
     * 使用 partitionName 查询 subject
     *
     * @param request request
     * @return subject
     */
    void querySubject(QuerySubjectRequest request, QuerySubjectCallback callback);

    void registerResponseSubscriber(ResponseSubscriber receiver);

    interface ResponseCallback<T> {

        void onSuccess(T response);
    }

    interface ResponseSubscriber extends ResponseCallback<MetaInfoResponse> {

        void onSuccess(MetaInfoResponse response);
    }

    interface QuerySubjectCallback extends ResponseCallback<QuerySubjectResponse> {

        void onSuccess(QuerySubjectResponse response);
    }
}
