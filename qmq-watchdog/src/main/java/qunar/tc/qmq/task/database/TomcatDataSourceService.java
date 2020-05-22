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

package qunar.tc.qmq.task.database;

import org.apache.tomcat.jdbc.pool.PoolConfiguration;
import org.apache.tomcat.jdbc.pool.PoolProperties;

import javax.sql.DataSource;

public class TomcatDataSourceService implements IDataSourceService {

    @Override
    public DataSource makeDataSource(String url, String driverClassName, String username, String pwd) {
        PoolConfiguration p = new PoolProperties();
        p.setMinIdle(0);
        p.setMaxActive(2);
        p.setMaxIdle(0);
        p.setInitialSize(0);
        p.setMaxWait(10000);
        p.setDriverClassName(driverClassName);
        p.setUrl(url);
        p.setUsername(username);
        p.setPassword(pwd);
        p.setValidationQuery("select 1");
        p.setTestOnBorrow(true);

        org.apache.tomcat.jdbc.pool.DataSource datasource = new org.apache.tomcat.jdbc.pool.DataSource();
        datasource.setPoolProperties(p);
        return datasource;
    }

    @Override
    public void close(DataSource dataSource) {
        if (dataSource instanceof org.apache.tomcat.jdbc.pool.DataSource) {
            ((org.apache.tomcat.jdbc.pool.DataSource) dataSource).close();
        }
    }
}
