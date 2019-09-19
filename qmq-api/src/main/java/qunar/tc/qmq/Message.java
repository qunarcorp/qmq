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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author miao.yang susing@gmail.com
 * @date 2012-12-26
 */
public interface Message {

    /**
     * 获取message id，一般是qmq自动生成的，message id需要保证全局唯一，不要将message id用于业务，一般仅做记录日志
     *
     * @return message id
     */
    String getMessageId();

    /**
     * 消息的主题
     *
     * @return subject
     */
    String getSubject();

    /**
     * broker 的存储 id
     *
     * @return partition name
     */
    String getPartitionName();

    /**
     * 消息的创建时间，指的是调用generateMessage方法的时间
     *
     * @return
     */
    Date getCreatedTime();

    Date getScheduleReceiveTime();

    void setProperty(String name, boolean value);

    void setProperty(String name, Boolean value);

    void setProperty(String name, int value);

    void setProperty(String name, Integer value);

    void setProperty(String name, long value);

    void setProperty(String name, Long value);

    void setProperty(String name, float value);

    void setProperty(String name, Float value);

    void setProperty(String name, double value);

    void setProperty(String name, Double value);

    void setProperty(String name, Date date);

    void setProperty(String name, String value);

    /**
     * 可以设置4MB的超大字符串，但是要注意，使用这个方法设置的字符串必须使用getLargeString方法获取
     * 另外，超大消息不提供持久化支持，不能使用事务或持久消息
     *
     * @param name  key
     * @param value value
     */
    void setLargeString(String name, String value);

    String getStringProperty(String name);

    boolean getBooleanProperty(String name);

    Date getDateProperty(String name);

    int getIntProperty(String name);

    long getLongProperty(String name);

    float getFloatProperty(String name);

    double getDoubleProperty(String name);

    /**
     * 获取setLargeString方法设置的字符串
     *
     * @param name key
     * @return value
     */
    String getLargeString(String name);

    /**
     * 这个方法不是线程安全
     *
     * @param tag 不能是null或empty, 长度不能超过Short.MAX_VALUE，tag的个数最多不能超过10个
     * @return this
     */
    Message addTag(String tag);

    /**
     * @return 默认返回empty set, 返回的是不可变set
     */
    Set<String> getTags();

    /**
     * @return
     * @deprecated 禁止使用该方法
     */
    @Deprecated
    Map<String, Object> getAttrs();

    /**
     * 期望显式的手动ack时，使用该方法关闭qmq默认的自动ack。
     * 该方法必须是在consumer端的MessageListener的onMessage方法入口处调用，否则会抛出异常
     * <p/>
     * 在producer端调用时会抛出UnsupportedOperationException异常
     *
     * @param auto
     */
    void autoAck(boolean auto);

    /**
     * 显式手动ack的时候，使用该方法
     *
     * @param elapsed 消息处理时长
     * @param e       如果消息处理失败请传入异常，否则传null
     *                <p/>
     *                在producer端调用会抛出UnsupportedOperationException异常
     */
    void ack(long elapsed, Throwable e);

    void ack(long elapsed, Throwable e, Map<String, String> attachment);


    void setDelayTime(Date date);

    void setDelayTime(long delayTime, TimeUnit timeUnit);

    /**
     * 设置用于顺序消息分组的key, 相同的 key 的消息可保证顺序消费模式下, 按照发送顺序消费
     * @param key key
     */
    void setOrderKey(String key);

    String getOrderKey();

    /**
     * 第几次发送
     * 使用方应该监控该次数，如果不是刻意设计该次数不应该太多
     *
     * @return
     */
    int times();

    void setMaxRetryNum(int maxRetryNum);

    int getMaxRetryNum();

    /**
     * 本地连续重试次数
     *
     * @return
     */
    int localRetries();

    void setStoreAtFailed(boolean storeAtFailed);

    void setDurable(boolean durable);

    boolean isDurable();
}
