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

package qunar.tc.qmq.producer;

/**
 * @author zhenyu.nie created on 2017 2017/7/5 11:57
 */
public class ConfigCenter {
    private static final ConfigCenter INSTANCE = new ConfigCenter();

    public static ConfigCenter getInstance() {
        return INSTANCE;
    }

    private static final int MIN_EXPIRED_TIME = 15;

    private int maxQueueSize = 10000;
    private int sendThreads = 3;
    private int sendBatch = 30;
    private long sendTimeoutMillis = 5000;
    private int sendTryCount = 10;

    private boolean syncSend = false;

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public int getSendThreads() {
        return sendThreads;
    }

    public void setSendThreads(int sendThreads) {
        this.sendThreads = sendThreads;
    }

    public int getSendBatch() {
        return sendBatch;
    }

    public void setSendBatch(int sendBatch) {
        this.sendBatch = sendBatch;
    }

    public long getSendTimeoutMillis() {
        return sendTimeoutMillis;
    }

    public int getSendTryCount() {
        return sendTryCount;
    }

    public void setSendTryCount(int sendTryCount) {
        this.sendTryCount = sendTryCount;
    }

    public boolean isSyncSend() {
        return syncSend;
    }

    public int getMinExpiredTime() {
        return MIN_EXPIRED_TIME;
    }
}
