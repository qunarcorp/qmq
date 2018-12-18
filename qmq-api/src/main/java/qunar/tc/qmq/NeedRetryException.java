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

package qunar.tc.qmq;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaohui.yu
 * 15/12/2
 * <p/>
 * qmq会根据该异常里的时间进行重试间隔控制
 */
public class NeedRetryException extends RuntimeException {
    private final long next;

    public NeedRetryException(Date next, String message) {
        super(message);
        this.next = next.getTime();
    }

    public NeedRetryException(int next, TimeUnit unit, String message) {
        super(message);
        this.next = System.currentTimeMillis() + unit.toMillis(next);
    }

    /**
     * WARNING WARNING
     * 使用该构造函数构造的异常会立即重试
     *
     * @param message
     */
    public NeedRetryException(String message) {
        this(new Date(), message);
    }

    public long getNext() {
        return next;
    }
}
