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

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import qunar.tc.qmq.consumer.MessageConsumerProvider;

class QmqClientBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {
    static final String QMQ_CLIENT_ANNOTATION = "QMQ_CLIENT_ANNOTATION";

    static final String DEFAULT_ID = "QMQ_CONSUMER_ALL";

    @Override
    protected Class<?> getBeanClass(Element element) {
        return MessageConsumerProvider.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        String appCode = element.getAttribute("appCode");
        String metaServer = element.getAttribute("metaServer");

        builder.addPropertyValue("appCode", appCode);
        builder.addPropertyValue("metaServer", metaServer);

        if (!parserContext.getRegistry().containsBeanDefinition(QMQ_CLIENT_ANNOTATION)) {
            GenericBeanDefinition scanner = new GenericBeanDefinition();
            scanner.setBeanClass(ConsumerAnnotationScanner.class);
            parserContext.getRegistry().registerBeanDefinition(QMQ_CLIENT_ANNOTATION, scanner);
        }
    }

    @Override
    protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext) {
        return DEFAULT_ID;
    }

}
