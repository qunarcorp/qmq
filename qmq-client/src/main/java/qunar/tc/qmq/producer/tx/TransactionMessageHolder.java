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

package qunar.tc.qmq.producer.tx;

import qunar.tc.qmq.MessageStore;
import qunar.tc.qmq.ProduceMessage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-5
 */
class TransactionMessageHolder {

    public void insertMessage(ProduceMessage message) {
        add(message);
    }

    List<ProduceMessage> get() {
        return queue;
    }

    private final MessageStore store;
    private List<ProduceMessage> queue = Collections.emptyList();

    TransactionMessageHolder(MessageStore store) {
        this.store = store;
    }

    private void add(ProduceMessage message) {
        if (queue.isEmpty())
            queue = new ArrayList<>(1);

        queue.add(message);
        message.setStore(store);
    }

}
