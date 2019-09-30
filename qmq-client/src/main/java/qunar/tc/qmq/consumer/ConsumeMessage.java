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

package qunar.tc.qmq.consumer;

import qunar.tc.qmq.Message;
import qunar.tc.qmq.base.BaseMessage;

import java.util.Map;

/**
 * @author yiqun.fan create on 17-8-19.
 */
public class ConsumeMessage extends BaseMessage {
    private transient volatile Thread processThread;
    private volatile boolean autoAck = true;
    private volatile transient int localRetries;
    private transient volatile Map<String, Object> filterContext;
    private transient volatile int processedFilterIndex = -1;

    protected ConsumeMessage(BaseMessage message) {
        super(message);
        this.processThread = Thread.currentThread();
    }

    public boolean isAutoAck() {
        return this.autoAck;
    }

    public void setProcessThread(Thread processThread) {
        this.processThread = processThread;
    }

    @Override
    public void autoAck(boolean auto) {
        if (processThread != Thread.currentThread()) {
            throw new RuntimeException("如需要使用显式ack，请在MessageListener的onMessage入口处调用autoAck设置，也不能在其他线程里调用");
        }
        this.autoAck = auto;
    }

    public Map<String, Object> filterContext() {
        return filterContext;
    }

    public void filterContext(Map<String, Object> filterContext) {
        this.filterContext = filterContext;
    }

    int processedFilterIndex() {
        return processedFilterIndex;
    }

    void processedFilterIndex(int processedFilterIndex) {
        this.processedFilterIndex = processedFilterIndex;
    }

    @Override
    public int localRetries() {
        return localRetries;
    }

    public void localRetries(int localRetries) {
        this.localRetries = localRetries;
    }

    @Override
    public Message addTag(String tag) {
        throw new UnsupportedOperationException("use addTag in producer only");
    }
}
