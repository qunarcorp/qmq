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

package qunar.tc.qmq.producer.tx.spring;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import qunar.tc.qmq.MessageStore;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.producer.tx.DefaultSqlStatementProvider;
import qunar.tc.qmq.producer.tx.SqlStatementProvider;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-9
 */
public class DefaultMessageStore implements MessageStore {
    private final JdbcTemplate platform;

    private final PreparedStatementCreatorFactory insertStatementFactory;

    private final Gson gson;

    private final RouterSelector routerSelector;

    private SqlStatementProvider sqlStatementProvider;

    DefaultMessageStore(DataSource datasource) {
        this(datasource, new NoopRouterSelector(), new DefaultSqlStatementProvider());
    }

    DefaultMessageStore(DataSource datasource, RouterSelector routerSelector) {
        this(datasource, routerSelector, new DefaultSqlStatementProvider());
    }

    DefaultMessageStore(DataSource datasource, SqlStatementProvider sqlStatementProvider) {
        this(datasource, new NoopRouterSelector(), sqlStatementProvider);
    }

    DefaultMessageStore(DataSource datasource, RouterSelector routerSelector, SqlStatementProvider sqlStatementProvider) {
        this.sqlStatementProvider = sqlStatementProvider;
        this.platform = new JdbcTemplate(datasource);
        this.insertStatementFactory = createFactory();
        this.gson = new GsonBuilder().setLongSerializationPolicy(LongSerializationPolicy.STRING).create();
        this.routerSelector = routerSelector;
        this.sqlStatementProvider = new DefaultSqlStatementProvider();
    }

    @Override
    public long insertNew(ProduceMessage message) {
        KeyHolder holder = new GeneratedKeyHolder();
        String json = this.gson.toJson(message.getBase());
        platform.update(this.insertStatementFactory.newPreparedStatementCreator(new Object[]{json, new Timestamp(System.currentTimeMillis())}), holder);
        message.setRouteKey(routerSelector.getRouteKey(platform.getDataSource()));
        return holder.getKey().longValue();
    }

    @Override
    public void finish(ProduceMessage message) {
        routerSelector.setRouteKey(message.getRouteKey(), platform.getDataSource());
        try {
            platform.update(sqlStatementProvider.getDeleteSql(), message.getSequence());
        } finally {
            routerSelector.clearRoute(message.getRouteKey(), platform.getDataSource());
        }
    }

    @Override
    public void block(ProduceMessage message) {
        routerSelector.setRouteKey(message.getRouteKey(), platform.getDataSource());
        try {
            platform.update(sqlStatementProvider.getBlockSql(), new Timestamp(System.currentTimeMillis()), message.getSequence());
        } finally {
            routerSelector.clearRoute(message.getRouteKey(), platform.getDataSource());
        }

    }

    @Override
    public void beginTransaction() {

    }


    @Override
    public void endTransaction() {

    }

    private PreparedStatementCreatorFactory createFactory() {
        List<SqlParameter> parameters = new ArrayList<>();
        parameters.add(new SqlParameter("content", Types.LONGVARCHAR));
        parameters.add(new SqlParameter("create_time", Types.TIMESTAMP));
        PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(sqlStatementProvider.getInsertSql(), parameters);
        factory.setReturnGeneratedKeys(true);
        return factory;
    }

    private static class NoopRouterSelector implements RouterSelector {

        @Override
        public Object getRouteKey(DataSource dataSource) {
            return null;
        }

        @Override
        public void setRouteKey(Object key, DataSource dataSource) {

        }

        @Override
        public void clearRoute(Object key, DataSource dataSource) {

        }
    }
}
