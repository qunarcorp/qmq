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


import java.net.URI;

/**
 * Created by IntelliJ IDEA. User: liuzz Date: 13-12-5 Time: 下午3:01
 */
public class MysqlMMMDatabaseDriver implements IDatabaseDriver {
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    private final IDataSourceService dataSourceService;


    public MysqlMMMDatabaseDriver() {
        dataSourceService = new TomcatDataSourceService();
    }

    public DatasourceWrapper makeDataSource(String url, String userName, String password) {
        String jdbcUrl = covertToJdbcUrl(url);
        return new DefaultDatasourceWrapper(dataSourceService.makeDataSource(jdbcUrl, DRIVER_CLASS, userName, password));
    }

    private String covertToJdbcUrl(String url) {
        URI uri = URI.create(url);
        return String.format("jdbc:mysql://%s:%s", uri.getHost(), uri.getPort());
    }

    @Override
    public void close(DatasourceWrapper dataSource) {
        if (dataSource == null) return;
        dataSourceService.close(dataSource.datasource());
    }
}
