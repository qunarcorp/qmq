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

import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

import static qunar.tc.qmq.consumer.annotation.QmqClientBeanDefinitionParser.DEFAULT_ID;
import static qunar.tc.qmq.consumer.annotation.QmqClientBeanDefinitionParser.QMQ_CLIENT_ANNOTATION;

/**
 * Created by zhaohui.yu
 * 2/4/17
 */
class QmqConsumerRegister implements ImportBeanDefinitionRegistrar {

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        AnnotationAttributes attributes = AnnotationAttributes.fromMap(importingClassMetadata.getAnnotationAttributes(EnableQmq.class.getName()));
        String appCode = attributes.getString("appCode");
        String metaServer = attributes.getString("metaServer");

        if (!registry.containsBeanDefinition(DEFAULT_ID)) {
            GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setBeanClass(MessageConsumerProvider.class);
            beanDefinition.setLazyInit(true);
            beanDefinition.setPropertyValues(propertyValues(appCode, metaServer));
            registry.registerBeanDefinition(DEFAULT_ID, beanDefinition);
        }

        if (!registry.containsBeanDefinition(QMQ_CLIENT_ANNOTATION)) {
            GenericBeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setBeanClass(ConsumerAnnotationScanner.class);
            registry.registerBeanDefinition(QMQ_CLIENT_ANNOTATION, beanDefinition);
        }
    }

    private MutablePropertyValues propertyValues(String appCode, String metaServer) {
        MutablePropertyValues propertyValues = new MutablePropertyValues();
        propertyValues.add("appCode", appCode);
        propertyValues.add("metaServer", metaServer);
        return propertyValues;
    }
}
