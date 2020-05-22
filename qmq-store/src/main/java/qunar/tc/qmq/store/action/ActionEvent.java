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

import qunar.tc.qmq.store.Action;

/**
 * @author keli.wang
 * @since 2017/10/16
 */
public class ActionEvent {
    private final long offset;
    private final Action action;

    public ActionEvent(final long offset, final Action action) {
        this.offset = offset;
        this.action = action;
    }

    public long getOffset() {
        return offset;
    }

    public Action getAction() {
        return action;
    }
}
