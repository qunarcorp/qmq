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

package qunar.tc.qmq.consumer.annotation;

import qunar.tc.qmq.TagType;

import java.lang.annotation.*;

/**
 * User: zhaohuiyu
 * Date: 7/5/13
 * Time: 7:28 PM
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
@Inherited
public @interface QmqConsumer {
    /**
     * (必填)订阅的主题, 支持${subject}形式注入
     */
    String subject();

    /**
     * (可选)consumerGroup,如果不填则为广播模式, 支持${group}形式注入
     */
    String consumerGroup() default "";

    /**
     * 注入ThreadPoolExecutor bean，消费消息逻辑在此线程池执行
     *
     * @return
     */
    String executor();

    /**
     * 是否是广播消息
     * 设成true时, 忽略consumerGroup的值
     */
    boolean isBroadcast() default false;

    /**
     * 是否是顺序消费
     */
    boolean isOrdered() default false;

    /**
     * 设成true时，每条消息(包括可靠)最多消费一次
     */
    boolean consumeMostOnce() default false;

    /**
     * 设置过滤tags。格式是{tag1, tag2, ...}
     * tags的长度为0时，过滤会失效，消费所有消息
     * 并且需要设置TagType，默认TagType是TagType.NO_TAG，tags会失效
     */
    String[] tags() default {};

    /**
     * 默认是TagType.NO_TAG
     * 如果TagType设置成TagType.NO_TAG, 则tags会失效，消费所有消息。
     * TagType.OR 表示以上设置的tags与消息的tags有交集
     * TagType.AND 表示以上设置的tags是消息的tags的子集
     */
    TagType tagType() default TagType.NO_TAG;

    /**
     * 幂等检查器beanName
     */
    String idempotentChecker() default "";

    /**
     * 过滤器beanName，按顺序
     */
    String[] filters() default {};
}
