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

package qunar.tc.qmq.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.store.Action;
import qunar.tc.qmq.store.Storage;
import qunar.tc.qmq.store.action.ActionEvent;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

/**
 * @author keli.wang
 * @since 2018/8/21
 */
public class OfflineActionHandler implements FixedExecOrderEventBus.Listener<ActionEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(OfflineActionHandler.class);

    private final Storage storage;

    public OfflineActionHandler(Storage storage) {
        this.storage = storage;
    }

    @Override
    public void onEvent(ActionEvent event) {
        switch (event.getAction().type()) {
            case FOREVER_OFFLINE:
                foreverOffline(event.getAction());
                break;
            default:
                break;

        }
    }

    private void foreverOffline(final Action action) {
        final String subject = action.subject();
        final String group = action.group();
        final String consumerId = action.consumerId();

        LOG.info("execute offline task, will remove pull log and checkpoint entry for {}/{}/{}", subject, group, consumerId);
        storage.destroyPullLog(subject, group, consumerId);
    }
}
