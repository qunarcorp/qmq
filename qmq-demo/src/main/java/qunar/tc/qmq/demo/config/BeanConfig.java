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

package qunar.tc.qmq.demo.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import qunar.tc.qmq.MessageProducer;
import qunar.tc.qmq.consumer.annotation.EnableQmq;
import qunar.tc.qmq.producer.MessageProducerProvider;
import qunar.tc.qmq.producer.tx.spring.SpringTransactionProvider;

import javax.sql.DataSource;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableQmq(appCode = "${appCode}", metaServer = "${metaServer}")
public class BeanConfig {

    @Bean
    public MessageProducer producer(@Value("${appCode}") String appCode,
                                    @Value("${metaServer}") String metaServer,
                                    @Autowired DataSource dataSource) {
        SpringTransactionProvider transactionProvider = new SpringTransactionProvider(dataSource);
        final MessageProducerProvider producer = new MessageProducerProvider(appCode, metaServer);
        producer.setTransactionProvider(transactionProvider);
        return producer;
    }

    /*
    处理消息的业务线程池，线程池队列不要设置太大
     */
    @Bean(name = "workerExecutor")
    public ThreadPoolExecutor workerExecutor(@Value("${executor.coreSize}") int core,
                                             @Value("${executor.maxSize}") int max,
                                             @Value("${executor.queueSize}") int queueSize) {
        return new ThreadPoolExecutor(core, max, 1,
                TimeUnit.MINUTES, new LinkedBlockingQueue<>(queueSize));
    }
}
