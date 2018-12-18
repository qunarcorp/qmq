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

package qunar.tc.qmq.task.model;

import java.util.Date;

public class MsgQueue {
    public final long id;
    public final String content;
    public final int error;
    public Date updateTime;

    public MsgQueue(long id, String content, int error, Date updateTime) {
        this.id = id;
        this.content = content;
        this.error = error;
        this.updateTime = updateTime;
    }
}
