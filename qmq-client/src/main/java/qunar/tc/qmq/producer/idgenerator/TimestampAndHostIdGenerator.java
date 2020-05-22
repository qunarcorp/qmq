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

package qunar.tc.qmq.producer.idgenerator;

import qunar.tc.qmq.utils.NetworkUtils;
import qunar.tc.qmq.utils.PidUtil;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: zhaohuiyu
 * Date: 6/4/13
 * Time: 10:06 AM
 */
public class TimestampAndHostIdGenerator implements IdGenerator {
    private static final int[] codex = {2, 3, 5, 6, 8, 9, 19, 11, 12, 14, 15, 17, 18};
    private static final AtomicInteger messageOrder = new AtomicInteger(0);

    private static final String localAddress = NetworkUtils.getLocalAddress();

    //在生成message id的时候带上进程id，避免一台机器上部署多个服务都发同样的消息时出问题
    private static final int PID = PidUtil.getPid();

    @Override
    public String getNext() {
        StringBuilder sb = new StringBuilder(40);
        long time = System.currentTimeMillis();
        String ts = new Timestamp(time).toString();

        for (int idx : codex)
            sb.append(ts.charAt(idx));
        sb.append('.').append(localAddress);
        sb.append('.').append(PID);
        sb.append('.').append(messageOrder.getAndIncrement()); //可能为负数.但是无所谓.
        return sb.toString();
    }
}
