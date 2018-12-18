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

package qunar.tc.qmq.meta.store;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;

import javax.sql.DataSource;

/**
 * @author keli.wang
 * @since 2017/9/28
 */
public class JdbcTemplateHolder {
    private static final Supplier<DataSource> DS_SUPPLIER = Suppliers.memoize(JdbcTemplateHolder::createDataSource);
    private static final Supplier<JdbcTemplate> SUPPLIER = Suppliers.memoize(JdbcTemplateHolder::createJdbcTemplate);
    private static final Supplier<TransactionTemplate> TRANS_SUPPLIER = Suppliers.memoize(JdbcTemplateHolder::createTransactionTemplate);

    private static JdbcTemplate createJdbcTemplate() {
        return new JdbcTemplate(DS_SUPPLIER.get());
    }

    private static TransactionTemplate createTransactionTemplate() {
        return new TransactionTemplate(new DataSourceTransactionManager(DS_SUPPLIER.get()));
    }

    private static DataSource createDataSource() {
        final DynamicConfig config = DynamicConfigLoader.load("datasource.properties");

        final HikariConfig cpConfig = new HikariConfig();
        cpConfig.setDriverClassName(config.getString("jdbc.driverClassName", "com.mysql.jdbc.Driver"));
        cpConfig.setJdbcUrl(config.getString("jdbc.url"));
        cpConfig.setUsername(config.getString("jdbc.username"));
        cpConfig.setPassword(config.getString("jdbc.password"));
        cpConfig.setMaximumPoolSize(config.getInt("pool.size.max", 10));

        return new HikariDataSource(cpConfig);
    }

    public static JdbcTemplate getOrCreate() {
        return SUPPLIER.get();
    }

}
