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

package qunar.tc.qmq.producer;

import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.service.exceptions.MessageException;

/**
 * @author zhenyu.nie created on 2017 2017/7/5 17:23
 */
public interface SendErrorHandler {

    void error(ProduceMessage pm, Exception e);

    void failed(ProduceMessage pm, Exception e);

    void block(ProduceMessage pm, MessageException ex);

    void finish(ProduceMessage pm, Exception e);
}
