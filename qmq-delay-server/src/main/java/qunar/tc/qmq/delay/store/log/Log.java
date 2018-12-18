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

package qunar.tc.qmq.delay.store.log;

import qunar.tc.qmq.delay.store.model.AppendLogResult;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-07-19 9:28
 */
public interface Log<R, T> {
    AppendLogResult<R> append(T record);

    boolean clean(Long key);

    void flush();
}
