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

package qunar.tc.qmq.jdbc;

import java.util.ServiceLoader;

import javax.sql.DataSource;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author keli.wang
 * @since 2017/9/28
 */
public class JdbcTemplateHolder {
    private static final Supplier<DataSource> DS_SUPPLIER = Suppliers.memoize(JdbcTemplateHolder::createDataSource);
    private static final Supplier<JdbcTemplate> SUPPLIER = Suppliers.memoize(JdbcTemplateHolder::createJdbcTemplate);

    private static JdbcTemplate createJdbcTemplate() {
        return new JdbcTemplate(DS_SUPPLIER.get());
    }

	private static DataSource createDataSource() {
		final DynamicConfig config = DynamicConfigLoader.load("datasource.properties");
		ServiceLoader<DataSourceFactory> factories = ServiceLoader.load(DataSourceFactory.class);
		for (DataSourceFactory factory : factories) {
			return factory.createDataSource(config);
		}

		return new DefaultDataSourceFactory().createDataSource(config);
	}

    public static JdbcTemplate getOrCreate() {
        return SUPPLIER.get();
    }

}
