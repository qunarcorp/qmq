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

package qunar.tc.qmq.store;

/**
 * Created by zhaohui.yu
 * 2020/6/7
 */
public class PullLogIndexEntry {
    public final long startOfPullLogSequence;

    public final long baseOfMessageSequence;

    public final int position;

    public final int num;

    public PullLogIndexEntry(final long startOfPullLogSequence, final long baseOfMessageSequence, int position, int num) {
        this.startOfPullLogSequence = startOfPullLogSequence;
        this.baseOfMessageSequence = baseOfMessageSequence;
        this.position = position;
        this.num = num;
    }

}
