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

package qunar.tc.qmq.delay.sender;

import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.delay.ScheduleIndex;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-08-07 13:24
 */
public interface DelayProcessor extends Disposable {
    void init();

    void send(ScheduleIndex index);
}
