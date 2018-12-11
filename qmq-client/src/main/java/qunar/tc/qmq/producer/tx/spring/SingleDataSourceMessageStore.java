/*
 * Copyright 2018 Qunar
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
 * limitations under the License.com.qunar.pay.trade.api.card.service.usercard.UserCardQueryFacade
 */

package qunar.tc.qmq.producer.tx.spring;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreatorFactory;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import qunar.tc.qmq.MessageStore;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.producer.tx.SqlConstant;

import javax.sql.DataSource;
import java.sql.Timestamp;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-9
 */
public class SingleDataSourceMessageStore implements MessageStore {
    private final JdbcTemplate platform;

    SingleDataSourceMessageStore(DataSource datasource) {
        this.platform = new JdbcTemplate(datasource);
    }

    @Override
    public long insertNew(ProduceMessage message) {
        PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(SqlConstant.insertSQL);
        KeyHolder holder = new GeneratedKeyHolder();
        platform.update(factory.newPreparedStatementCreator(new Object[]{null, System.currentTimeMillis()}), holder);
        return holder.getKey().longValue();
    }

    @Override
    public void finish(ProduceMessage message) {
        platform.update(SqlConstant.finishSQL, message.getSequence());
    }

    @Override
    public void block(ProduceMessage message) {
        platform.update(SqlConstant.blockSQL, new Timestamp(System.currentTimeMillis()), message.getSequence());
    }

    @Override
    public void beginTransaction() {

    }


    @Override
    public void endTransaction() {

    }
}
