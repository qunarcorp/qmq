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

package qunar.tc.qmq.producer.sender;

import com.google.common.util.concurrent.ListenableFuture;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.List;
import java.util.Map;

/**
 * @author zhenyu.nie created on 2017 2017/7/3 12:25
 */
public interface Connection {

    Map<String, MessageException> send(List<ProduceMessage> messages) throws Exception;

    ListenableFuture<Map<String, MessageException>> sendAsync(List<ProduceMessage> messages) throws Exception;

    void destroy();
}
