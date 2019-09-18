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
package qunar.tc.qmq.base;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-26
 */
public class BaseMessage implements Message, Serializable {
    private static final long serialVersionUID = 303069262539600333L;

    private static final int MAX_MESSAGE_ID_LEN = 100;

    private static final int MAX_TAGS_COUNT = 10;

    String messageId;

    String subject;

    private final Set<String> tags = new CopyOnWriteArraySet<>();

    transient boolean isBigMessage = false;
    private boolean storeAtFailed;
    private boolean durable = true;

    public enum keys {
        qmq_createTime,
        qmq_expireTime,
        qmq_consumerGroupName,
        qmq_scheduleReceiveTime,
        qmq_times,
        qmq_maxRetryNum,
        qmq_appCode,
        qmq_pullOffset,
        qmq_corruptData,
        qmq_env,
        qmq_subEnv,
        qmq_orderKey,
        qmq_orderStrategy,
        qmq_logicPartition,
        qmq_partitionName,
        qmq_subject,
        qmq_partitionVersion,
        qmq_partitionBroker
    }

    private static final Set<String> keyNames = Sets.newHashSet();

    static {
        for (keys key : keys.values())
            keyNames.add(key.name());
    }

    HashMap<String, Object> attrs = new HashMap<>();

    public BaseMessage() {
    }

    public BaseMessage(String messageId, String subject) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(messageId), "message id should not empty");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(subject), "message subject should not empty");
        Preconditions.checkArgument(messageId.length() <= MAX_MESSAGE_ID_LEN, "messageId长度不能超过" + MAX_MESSAGE_ID_LEN + "个字符");
        if (RetryPartitionUtils.isRealPartitionName(subject)) {
            Preconditions.checkArgument(subject.length() <= MAX_MESSAGE_ID_LEN, "subject长度不能超过" + MAX_MESSAGE_ID_LEN + "个字符");
        }

        this.messageId = messageId;
        this.subject = subject;
        long time = System.currentTimeMillis();
        setProperty(keys.qmq_createTime, time);
    }

    public BaseMessage(BaseMessage message) {
        this(message.getMessageId(), message.getSubject());
        this.tags.addAll(message.getTags());
        this.attrs = new HashMap<>(message.attrs);
    }

    public Map<String, Object> getAttrs() {
        return Collections.unmodifiableMap(attrs);
    }

    @Deprecated
    public void setAttrs(HashMap<String, Object> attrs) {
        this.attrs = attrs;
    }

    @Override
    public String getMessageId() {
        return messageId;
    }

    @Override
    public String getSubject() {
        return subject;
    }

    @Override
    public String getPartitionName() {
        return getStringProperty(keys.qmq_partitionName);
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public void setPartitionName(String partitionName) {
        this.setProperty(keys.qmq_partitionName, partitionName);
    }

    @Override
    public Date getCreatedTime() {
        return getDateProperty(keys.qmq_createTime.name());
    }

    public void setProperty(keys key, boolean value) {
        attrs.put(key.name(), Boolean.valueOf(value));
    }

    public void setProperty(keys key, String value) {
        attrs.put(key.name(), value);
    }

    public void setProperty(keys key, int value) {
        attrs.put(key.name(), value);
    }

    public void setProperty(keys key, long value) {
        attrs.put(key.name(), value);
    }

    public void setProperty(keys key, Date value) {
        attrs.put(key.name(), value.getTime());
    }

    /**
     * 为了类型属性的稳定此方法一定不能暴漏成public.
     */
    private void setObjectProperty(String name, Object value) {
        if (keyNames.contains(name))
            throw new IllegalArgumentException("property name [" + name + "] is protected. ");
        if (name == null || name.length() == 0) return;
        attrs.put(name, value);
    }

    @Override
    public void setProperty(String name, boolean value) {
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, Boolean value) {
        if (value == null) return;
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, int value) {
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, Integer value) {
        if (value == null) return;
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, long value) {
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, Long value) {
        if (value == null) return;
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, float value) {
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, Float value) {
        if (value == null) return;
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, double value) {
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, Double value) {
        if (value == null) return;
        setObjectProperty(name, value);
    }

    @Override
    public void setProperty(String name, Date value) {
        if (value == null) return;
        setObjectProperty(name, value.getTime());
    }

    @Override
    public void setProperty(String name, String value) {
        if (value == null) return;
        setObjectProperty(name, value);
    }

    @Override
    public void setLargeString(String name, String value) {
        LargeStringUtil.setLargeString(this, name, value);
    }

    @Override
    public String getStringProperty(String name) {
        return valueOfString(attrs.get(name));
    }

    @Override
    public boolean getBooleanProperty(String name) {
        Object v = attrs.get(name);
        if (v == null)
            return false;
        return Boolean.valueOf(v.toString());
    }

    @Override
    public Date getDateProperty(String name) {
        Object o = attrs.get(name);
        if (o == null)
            return null;
        Long v = Long.valueOf(o.toString());
        return new Date(v);
    }

    @Override
    public int getIntProperty(String name) {
        Object o = attrs.get(name);
        if (o == null)
            return 0;
        return Integer.valueOf(o.toString());
    }

    @Override
    public long getLongProperty(String name) {
        Object o = attrs.get(name);
        if (o == null)
            return 0;
        return Long.valueOf(o.toString());
    }

    @Override
    public float getFloatProperty(String name) {
        Object o = attrs.get(name);
        if (o == null)
            return 0;
        return Float.valueOf(o.toString());
    }

    @Override
    public double getDoubleProperty(String name) {
        Object o = attrs.get(name);
        if (o == null)
            return 0;
        return Double.valueOf(o.toString());
    }

    @Override
    public String getLargeString(String name) {
        return LargeStringUtil.getLargeString(this, name);
    }

    private static String valueOfString(Object str) {
        return str == null ? null : str.toString();
    }

    public Object getProperty(keys key) {
        return attrs.get(key.name());
    }

    @Override
    public void setOrderKey(String key) {
        setProperty(keys.qmq_orderKey, key);
    }

    @Override
    public String getOrderKey() {
        return getStringProperty(keys.qmq_orderKey);
    }

    public String getStringProperty(keys key) {
        return getStringProperty(key.name());
    }

    public void removeProperty(keys key) {
        attrs.remove(key.name());
    }

    @Override
    public Message addTag(String tag) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(tag), "a tag can not be null or empty");
        Preconditions.checkArgument(tag.length() <= Short.MAX_VALUE, "the length of a tag mush be smaller than Short.MAX_VALUE");
        if (tags.size() >= MAX_TAGS_COUNT) {
            throw new IllegalArgumentException("the size of tags cannot be more than MAX_TAGS_COUNT(" + MAX_TAGS_COUNT + ")");
        }
        tags.add(tag);
        return this;
    }

    @Override
    public Set<String> getTags() {
        return Collections.unmodifiableSet(tags);
    }

    @Override
    public void autoAck(boolean auto) {
        throw new UnsupportedOperationException("请在consumer端设置auto ack");
    }

    @Override
    public void ack(long elapsed, Throwable e) {
        ack(elapsed, e, null);
    }

    @Override
    public void ack(long elapsed, Throwable e, Map<String, String> attachment) {
        throw new UnsupportedOperationException("BaseMessage does not support this method");
    }

    public void setExpiredTime(long time) {
        setProperty(keys.qmq_expireTime, time);
    }

    public void setExpiredDelay(long timeDelay, TimeUnit timeUnit) {
        setExpiredTime(System.currentTimeMillis() + timeUnit.toMillis(timeDelay));
    }

    @Override
    public void setDelayTime(Date date) {
        Preconditions.checkNotNull(date, "消息定时接收时间不能为空");
        long time = date.getTime();
        Preconditions.checkArgument(time > System.currentTimeMillis(), "消息定时接收时间不能为过去时");
        setDelay(time);
    }

    // WARNING setProperty(String
    // name,...)这个版本的方法里面会对name进行检查，如果这个name在keys集合(qmq内部使用)
    // 中则会抛出异常，这是为了防止业务使用到这些内部保留关键字。
    // 所以qmq内部使用的属性都应该使用setProperty(keys key,...)这个版本。
    private void setDelay(long time) {
        setProperty(keys.qmq_scheduleReceiveTime, time);
    }

    @Override
    public void setDelayTime(long delayTime, TimeUnit timeUnit) {
        Preconditions.checkNotNull(timeUnit, "消息延迟接收时间单位不能为空");
        Preconditions.checkArgument(delayTime >= 0, "消息延迟接收时间不能为过去时");

        long sendTime = System.currentTimeMillis() + timeUnit.toMillis(delayTime);
        setDelay(sendTime);
    }

    @Override
    public Date getScheduleReceiveTime() {
        return getDateProperty(keys.qmq_scheduleReceiveTime.name());
    }

    @Override
    public int times() {
        Object o = getProperty(keys.qmq_times);
        if (o == null) return 1;
        return Integer.valueOf(o.toString());
    }

    @Override
    public void setMaxRetryNum(int maxRetryNum) {
        setProperty(keys.qmq_maxRetryNum, maxRetryNum);
    }

    @Override
    public int getMaxRetryNum() {
        String value = getStringProperty(keys.qmq_maxRetryNum);
        if (Strings.isNullOrEmpty(value)) {
            return -1;
        }
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return -1;
        }
    }

    @Override
    public int localRetries() {
        throw new UnsupportedOperationException("本地重试，只有消费端才支持");
    }

    @Override
    public void setStoreAtFailed(boolean storeAtFailed) {
        this.storeAtFailed = storeAtFailed;
    }

    public boolean isStoreAtFailed() {
        return this.storeAtFailed;
    }

    @Override
    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    @Override
    public boolean isDurable() {
        return this.durable;
    }

}
