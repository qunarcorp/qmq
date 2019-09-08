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

package qunar.tc.qmq;

/**
 * Created by zhaohui.yu
 * 10/31/16
 */
public interface ProduceMessage extends Retriable {

    String getMessageId();

    String getSubject();

    void send();

    void error(Exception e);

    void failed();

    void block();

    void finish();

    void reset();

    Message getBase();

    void startSendTrace();

    void setStore(MessageStore messageStore);

    void save();

    long getSequence();

    void setRouteKey(Object routeKey);

    Object getRouteKey();

    void setMessageGroup(MessageGroup messageGroup);

    MessageGroup getMessageGroup();
}
