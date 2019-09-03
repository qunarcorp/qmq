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

import java.util.List;
import java.util.concurrent.Future;

/**
 * @author yiqun.fan create on 17-9-11.
 */
public interface PullConsumer extends BaseConsumer, AutoCloseable, PullClient {

    long MAX_PULL_TIMEOUT_MILLIS = Long.MAX_VALUE / 2;  // 最长拉取超时时间
    boolean DEFAULT_RESET_CREATE_TIME = false;

    void online();

    void offline();

    /**
     * 设置true时，每条消息(包括可靠)只消费一次，拉取后自动ack，无需调用ack方法。
     * 设置false时，可靠消息不会自动ack，需要调用ack方法，非可靠不需要。
     * 默认值是false。
     */
    void setConsumeMostOnce(boolean consumeMostOnce);

    /**
     * 返回是否设置了consumeMostOnce
     */
    boolean isConsumeMostOnce();

    /**
     * 参考 qunar.tc.qmq.consumer.annotation.QmqConsumer#filterTags()
     */
    // TODO consumer
    //void setFilterTags(String[] filterTags);

    // TODO consumer
    //String[] filterTags();

    /**
     * 拉取到size个消息后才返回。
     * 如果producer发送的消息个数没有达到size，则pull方法会被阻塞住。
     * 如果当前拉取线程被Interrupted, 则返回已经拉取到的消息。
     * 返回的消息个数可能不等于size。
     * <p>
     * 对于可靠消息处理完成后，必须调用Message的ack方法，然后再次拉取。
     * 对于非可靠消息，无需调用ack方法。
     */
    List<Message> pull(int size);

    /**
     * 尝试在timeoutMillis内拉取size个消息。
     * 返回的消息个数可能不等于size。
     * 如果size <= 0，会立即返回一个空的List<Message>。
     * <p>
     * 当broker上剩余的消息数小于size时，pull方法会阻塞至超时。
     * 实际调用时间可能大于timeoutMillis。
     * timeoutMillis最小值是1000，应尽量设置大的timeout。
     * <p>
     * 如果timeoutMillis设置为小于0的时候，则表示不管队列里有没有消息都拉一下立即返回，
     * 这样实际返回的消息条数可能小于size，但是不会大于size
     * <p>
     * 对于可靠消息处理完成后，必须调用Message的ack方法，然后再次拉取。
     * 对于非可靠消息，无需调用ack方法。
     */
    List<Message> pull(int size, long timeoutMillis);

    /**
     * 返回的Future不支持cancel()方法，调用cancel()会抛出UnsupportedOperationException异常。
     * 如果调用Future的get方法发生超时，必须在之后调用get()直到isDone()返回true。
     * <p>
     * 对于可靠消息处理完成后，必须调用Message的ack方法，然后再次拉取。
     * 对于非可靠消息，无需调用ack方法。
     */
    Future<List<Message>> pullFuture(int size);

    /**
     * 实际的拉取时间可能超过timeoutMillis，所以必须等到isDone()返回true才可以丢弃返回的future，
     * 否则可能出现拉取到的消息没有被获取到。
     * timeoutMillis的设置参考 qunar.tc.qmq.PullConsumer#pull(int, long) 的注释
     * <p>
     * 对于可靠消息处理完成后，必须调用Message的ack方法，然后再次拉取。
     * 对于非可靠消息，无需调用ack方法。
     */
    Future<List<Message>> pullFuture(int size, long timeoutMillis);

    Future<List<Message>> pullFuture(int size, long timeoutMillis, boolean isResetCreateTime);

    String getClientId();
}
