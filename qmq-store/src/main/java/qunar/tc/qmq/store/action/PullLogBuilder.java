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

package qunar.tc.qmq.store.action;

import qunar.tc.qmq.store.Storage;
import qunar.tc.qmq.store.PullLogMessage;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

import java.util.ArrayList;
import java.util.List;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class PullLogBuilder implements FixedExecOrderEventBus.Listener<ActionEvent> {
    private final Storage storage;

    public PullLogBuilder(final Storage storage) {
        this.storage = storage;
    }

    @Override
    public void onEvent(final ActionEvent event) {
        switch (event.getAction().type()) {
            case PULL:
                buildPullLog(event);
                break;
        }
    }

    private void buildPullLog(ActionEvent event) {
        final PullAction action = (PullAction) event.getAction();
        if (action.isExclusiveConsume()) return;

        if (action.getFirstSequence() - action.getLastSequence() > 0) return;
        storage.putPullLogs(action.subject(), action.group(), action.consumerId(), createMessages(action));
    }

    private List<PullLogMessage> createMessages(final PullAction action) {
        final int count = (int) (action.getLastSequence() - action.getFirstSequence() + 1);
        final List<PullLogMessage> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            messages.add(new PullLogMessage(action.getFirstSequence() + i, action.getFirstMessageSequence() + i));
        }

        return messages;
    }
}
