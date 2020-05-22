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

import qunar.tc.qmq.store.CheckpointManager;
import qunar.tc.qmq.store.event.FixedExecOrderEventBus;

/**
 * @author keli.wang
 * @since 2017/8/21
 */
public class MaxSequencesUpdater implements FixedExecOrderEventBus.Listener<ActionEvent> {
    private final CheckpointManager manager;

    public MaxSequencesUpdater(final CheckpointManager manager) {
        this.manager = manager;
    }

    @Override
    public void onEvent(final ActionEvent event) {
        final long offset = event.getOffset();

        switch (event.getAction().type()) {
            case PULL:
                manager.updateActionReplayState(offset, (PullAction) event.getAction());
                break;
            case RANGE_ACK:
                manager.updateActionReplayState(offset, (RangeAckAction) event.getAction());
                break;
        }

    }
}
