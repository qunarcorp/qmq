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

import io.netty.util.internal.PlatformDependent;
import qunar.tc.qmq.monitor.QMon;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Created by zhaohui.yu
 * 4/11/18
 */
abstract class ReferenceObject {
    private static final AtomicIntegerFieldUpdater<ReferenceObject> REF_CNT_UPDATER;

    static {
        AtomicIntegerFieldUpdater<ReferenceObject> updater =
                PlatformDependent.newAtomicIntegerFieldUpdater(ReferenceObject.class, "refCnt");
        if (updater == null) {
            updater = AtomicIntegerFieldUpdater.newUpdater(ReferenceObject.class, "refCnt");
        }
        REF_CNT_UPDATER = updater;
    }

    private volatile int refCnt = 1;

    public boolean retain() {
        for (; ; ) {
            final int refCnt = this.refCnt;
            if (refCnt < 1) {
                return false;
            }

            if (REF_CNT_UPDATER.compareAndSet(this, refCnt, refCnt + 1)) {
                QMon.logSegmentTotalRefCountInc();
                return true;
            }
        }
    }

    public boolean release() {
        for (; ; ) {
            final int refCnt = this.refCnt;
            if (refCnt < 1) {
                return true;
            }

            if (REF_CNT_UPDATER.compareAndSet(this, refCnt, refCnt - 1)) {
                final boolean allRefReleased = refCnt == 1;
                if (!allRefReleased) {
                    QMon.logSegmentTotalRefCountDec();
                }
                return allRefReleased;
            }
        }
    }
}
