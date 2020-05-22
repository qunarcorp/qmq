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

package qunar.tc.qmq.consumer.idempotent;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import qunar.tc.qmq.IdempotentChecker;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.base.BaseMessage;

import java.util.Date;

/**
 * Created by zhaohui.yu
 * 15/11/25
 */
public abstract class AbstractIdempotentChecker implements IdempotentChecker {

    private final KeyExtractor extractor;

    public AbstractIdempotentChecker(KeyExtractor extractor) {
        this.extractor = extractor;
    }

    @Override
    public final boolean isProcessed(Message message) {
        try {
            return doIsProcessed(message);
        } catch (Exception e) {
            if (isIgnoreOnFailed()) {
                return false;
            }
            throw new RuntimeException(e);
        }
    }

    protected abstract boolean doIsProcessed(Message message) throws Exception;

    @Override
    public final void markProcessed(Message message, Throwable e) {
        if (e == null) {
            markProcessed(message);
            return;
        }

        if (e instanceof Exception) {
            //忽略某些异常
            if (idempotentFor != null) {
                if (idempotentFor.isAssignableFrom(e.getClass())) {
                    markProcessed(message);
                    return;
                }
            }

            //只有指定的异常回滚
            if (retryFor != null) {
                if (retryFor.isAssignableFrom(e.getClass())) {
                    markFailed(message);
                    return;
                }
            }
            markFailed(message);
        } else {
            markProcessed(message);
        }
    }

    protected abstract void markFailed(Message message);

    protected abstract void markProcessed(Message message);

    private Class<Exception> retryFor;
    private Class<Exception> idempotentFor;
    private boolean ignoreOnFailed;

    public final void setRetryFor(Class<Exception> e) {
        this.retryFor = e;
    }

    public final void setIdempotentFor(Class<Exception> e) {
        this.idempotentFor = e;
    }

    private boolean needRetry(Exception e) {
        if (retryFor == null) return true;
        return (retryFor.isAssignableFrom(e.getClass()));
    }

    public final boolean isIdempotent(Exception e) {
        if (idempotentFor == null) return false;
        return idempotentFor.isAssignableFrom(e.getClass());
    }

    /**
     * 这个地方将subject, consumerGroup拼接上去太长了
     * 最好是能将这个长串映射为一个整型带过来
     *
     * @param message
     * @return
     */
    protected String keyOf(Message message) {
        String original = extractor.extract(message);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(original), "使用所提供的keyFunc无法提取幂等key");

        return message.getSubject() + "%" + message.getStringProperty(BaseMessage.keys.qmq_consumerGroupName.name()) + "%" + original;
    }


    public final void setIgnoreOnFailed(boolean ignoreOnFailed) {
        this.ignoreOnFailed = ignoreOnFailed;
    }

    protected final boolean isIgnoreOnFailed() {
        return this.ignoreOnFailed;
    }

    public abstract void garbageCollect(Date before);

    public interface KeyExtractor {
        String extract(Message message);
    }

    public static KeyExtractor DEFAULT_EXTRACTOR = new KeyExtractor() {
        @Override
        public String extract(Message message) {
            return message.getMessageId();
        }
    };

}
