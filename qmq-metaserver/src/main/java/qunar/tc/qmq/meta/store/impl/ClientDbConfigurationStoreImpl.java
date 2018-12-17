package qunar.tc.qmq.meta.store.impl;

import org.springframework.jdbc.core.JdbcTemplate;
import qunar.tc.qmq.meta.model.ClientDbInfo;
import qunar.tc.qmq.meta.store.ClientDbConfigurationStore;
import qunar.tc.qmq.meta.store.JdbcTemplateHolder;

import java.sql.Timestamp;

public class ClientDbConfigurationStoreImpl implements ClientDbConfigurationStore {

    private final JdbcTemplate jdbcTemplate;

    public ClientDbConfigurationStoreImpl() {
        this.jdbcTemplate = JdbcTemplateHolder.getOrCreate();
    }

    @Override
    public void insertDb(ClientDbInfo clientDbInfo) {
        jdbcTemplate.update("insert into ingore datasource_config(url,username,password,create_time) values(?,?,?,?)",
                clientDbInfo.getUrl(), clientDbInfo.getUserName(), clientDbInfo.getPassword(), new Timestamp(System.currentTimeMillis()));
    }
}
