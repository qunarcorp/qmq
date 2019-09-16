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

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.AbstractBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import qunar.tc.qmq.*;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.Executor;

import static qunar.tc.qmq.consumer.annotation.QmqClientBeanDefinitionParser.DEFAULT_ID;

class ConsumerAnnotationScanner implements BeanPostProcessor, ApplicationContextAware, BeanFactoryAware {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerAnnotationScanner.class);

    private static final Set<Method> registeredMethods = new HashSet<Method>();

    private ApplicationContext context;

    private AbstractBeanFactory beanFactory;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        parseMethods(bean, bean.getClass().getDeclaredMethods());
        return bean;
    }

    private void parseMethods(final Object bean, Method[] methods) {
        String beanName = bean.getClass().getCanonicalName();

        for (final Method method : methods) {
            if (registeredMethods.contains(method)) continue;

            QmqConsumer annotation = AnnotationUtils.findAnnotation(method, QmqConsumer.class);
            if (annotation == null) continue;

            if (!Modifier.isPublic(method.getModifiers())) {
                throw new RuntimeException("标记@QmqConsumer的方法必须是public的");
            }

            String methodName = method.getName();
            Class[] args = method.getParameterTypes();
            String message = String.format("如果想配置成为message listener,方法必须有且只有一个参数,类型必须为qunar.tc.qmq.Message类型: %s method:%s", beanName, methodName);
            if (args.length != 1) {
                logger.error(message);
                throw new RuntimeException(message);
            }
            if (args[0] != Message.class) {
                logger.error(message);
                throw new RuntimeException(message);
            }

            String subject = resolve(annotation.subject());

            if (Strings.isNullOrEmpty(subject)) {
                String err = String.format("使用@QmqConsumer,必须提供subject, class:%s method:%s", beanName, methodName);
                logger.error(err);
                throw new RuntimeException(err);
            }
            registeredMethods.add(method);

            String consumerGroup = annotation.isBroadcast() ? "" : resolve(annotation.consumerGroup());
            ListenerHolder listenerHolder = new ListenerHolder(context, beanFactory, bean, method, subject, consumerGroup, annotation.executor(), buildSubscribeParam(annotation), annotation.idempotentChecker(), annotation.filters());
            listenerHolder.registe();
        }
    }

    private SubscribeParam buildSubscribeParam(QmqConsumer annotation) {
        return new SubscribeParam.SubscribeParamBuilder()
                .setConsumeMostOnce(annotation.consumeMostOnce())
                .setTags(new HashSet<>(Arrays.asList(annotation.tags())))
                .setTagType(annotation.tagType())
                .setBroadcast(annotation.isBroadcast())
                .setOrdered(annotation.isOrdered())
                .create();
    }

    private String resolve(String value) {
        if (Strings.isNullOrEmpty(value)) return value;
        if (beanFactory == null) return value;
        return beanFactory.resolveEmbeddedValue(value);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        if (beanFactory instanceof AbstractBeanFactory) {
            this.beanFactory = (AbstractBeanFactory) beanFactory;
        }
    }

    private static class ListenerHolder {
        private final ApplicationContext context;
        private final MessageListener listener;
        private final AbstractBeanFactory beanFactory;
        private final String subject;
        private final String group;
        private final String executorName;
        private final SubscribeParam subscribeParam;

        private ListenerHolder(ApplicationContext context,
                               AbstractBeanFactory beanFactory,
                               Object bean,
                               Method method,
                               String subject,
                               String group,
                               String executor,
                               SubscribeParam subscribeParam,
                               String idempotentChecker,
                               String[] filters) {
            this.context = context;
            this.beanFactory = beanFactory;
            this.subject = subject;
            this.group = group;
            this.executorName = executor;
            this.subscribeParam = subscribeParam;
            IdempotentChecker idempotentCheckerBean = null;
            if (idempotentChecker != null && idempotentChecker.length() > 0) {
                idempotentCheckerBean = context.getBean(idempotentChecker, IdempotentChecker.class);
            }

            List<Filter> filterBeans = new ArrayList<>();
            if (filters != null && filters.length > 0) {
                for (int i = 0; i < filters.length; ++i) {
                    filterBeans.add(context.getBean(filters[i], Filter.class));
                }
            }
            this.listener = new GeneratedListener(bean, method, idempotentCheckerBean, filterBeans);
        }

        public void registe() {
            MessageConsumerProvider consumer = resolveConsumer();
            Executor executor = resolveExecutor(executorName);
            consumer.addListener(subject, group, listener, executor, subscribeParam);
        }

        private MessageConsumerProvider resolveConsumer() {
            MessageConsumerProvider result = null;

            if (beanFactory != null) {
                result = beanFactory.getBean(DEFAULT_ID, MessageConsumerProvider.class);
            }
            if (result != null) return result;

            if (context != null) {
                result = context.getBean(DEFAULT_ID, MessageConsumerProvider.class);
            }
            if (result != null) return result;

            throw new RuntimeException("没有正确的配置qmq，如果使用Springboot请确保升级到了最新版本");
        }

        private Executor resolveExecutor(String executorName) {
            Executor executor = null;

            if (beanFactory != null) {
                executor = beanFactory.getBean(executorName, Executor.class);
            }

            if (executor != null) return executor;

            if (context != null) {
                executor = context.getBean(executorName, Executor.class);
            }
            if (executor != null) return executor;

            throw new RuntimeException("处理消息的线程池必须配置");
        }
    }
}
