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
        jdbcTemplate.update("INSERT IGNORE INTO datasource_config(url,user_name,password,room,create_time) VALUES(?,?,?,?)",
                clientDbInfo.getUrl(), clientDbInfo.getUserName(), clientDbInfo.getPassword(), clientDbInfo.getRoom(), new Timestamp(System.currentTimeMillis()));
    }
}
