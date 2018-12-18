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

package qunar.tc.qmq.task;

import org.springframework.jdbc.core.JdbcTemplate;
import qunar.tc.qmq.MessageProducer;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.producer.MessageProducerProvider;
import qunar.tc.qmq.task.database.DatabaseDriverMapping;
import qunar.tc.qmq.task.database.IDatabaseDriver;
import qunar.tc.qmq.task.database.MysqlMMMDatabaseDriver;
import qunar.tc.qmq.task.database.TomcatDataSourceService;
import qunar.tc.qmq.task.store.IDataSourceConfigStore;
import qunar.tc.qmq.task.store.impl.CachedMessageClientStore;
import qunar.tc.qmq.task.store.impl.DataSourceConfigStoreImpl;
import qunar.tc.qmq.task.store.impl.LeaderElectionDaoImpl;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class Bootstrap {
    public static void main(String[] args) throws InterruptedException {
        DynamicConfig config = DynamicConfigLoader.load("watchdog.properties");
        int sendMessageTaskExecuteTimeout = config.getInt("sendMessageTaskExecuteTimeout", 3 * 60 * 1000);
        int refreshInterval = config.getInt("refreshInterval", 3 * 60 * 1000);
        int checkInterval = config.getInt("checkInterval", 60 * 1000);
        String namespace = config.getString("namespace", "default");
        CachedMessageClientStore cachedMessageClientStore = new CachedMessageClientStore();
        JdbcTemplate jdbcTemplate = createJdbcTemplate();
        IDataSourceConfigStore dataSourceConfigStore = new DataSourceConfigStoreImpl(jdbcTemplate);
        TaskManager taskManager = new TaskManager(sendMessageTaskExecuteTimeout, refreshInterval, checkInterval, namespace,
                cachedMessageClientStore, dataSourceConfigStore, initDriverMapping(), createMessageProducer(config));
        Tasks tasks = new Tasks(namespace, taskManager, new LeaderElectionDaoImpl(jdbcTemplate));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> tasks.destroy()));
        tasks.start();

        CountDownLatch waiter = new CountDownLatch(1);
        waiter.await();
    }

    private static MessageProducer createMessageProducer(DynamicConfig config) {
        MessageProducerProvider producer = new MessageProducerProvider();
        producer.setAppCode(config.getString("appCode"));
        producer.setMetaServer(config.getString("meta.server.endpoint"));
        producer.init();
        return producer;
    }

    private static DatabaseDriverMapping initDriverMapping() {
        Map<String, IDatabaseDriver> mapping = new HashMap<>();
        mapping.put("mmm", new MysqlMMMDatabaseDriver());
        return new DatabaseDriverMapping(mapping);
    }

    private static JdbcTemplate createJdbcTemplate() {
        DynamicConfig config = DynamicConfigLoader.load("datasource.properties");
        TomcatDataSourceService dataSourceService = new TomcatDataSourceService();
        String jdbcUrl = config.getString("jdbc.url");
        String jdbcDriver = config.getString("jdbc.driverClassName");
        String userName = config.getString("jdbc.username");
        String password = config.getString("jdbc.password");
        DataSource dataSource = dataSourceService.makeDataSource(jdbcUrl, jdbcDriver, userName, password);
        return new JdbcTemplate(dataSource);
    }
}
