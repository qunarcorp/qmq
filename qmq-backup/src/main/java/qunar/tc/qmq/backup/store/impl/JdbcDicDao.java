package qunar.tc.qmq.backup.store.impl;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import qunar.tc.qmq.backup.store.DicStore;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;

import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * @author yiqun.fan create on 17-10-31.
 */
public class JdbcDicDao implements DicStore {
    private final JdbcTemplate jdbcTemplate = JdbcTemplateHolder.getOrCreate();
    private final String getIdSql;
    private final String getNameSql;
    private final String insertNameSql;

    public JdbcDicDao(String table) {
        getIdSql = String.format("SELECT id FROM %s WHERE name =?", table);
        getNameSql = String.format("SELECT name FROM %s WHERE id=?", table);
        insertNameSql = String.format("INSERT INTO %s(name) VALUES(?)", table);
    }

    @Override
    public String getName(int id) {
        return jdbcTemplate.queryForObject(getNameSql, new Object[]{id}, String.class);
    }

    @Override
    public int getId(String name) {
        return jdbcTemplate.queryForObject(getIdSql, new Object[]{name}, Integer.class);
    }

    @Override
    public int insertName(String name) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        PreparedStatementCreator psc = connection -> {
            PreparedStatement ps = connection.prepareStatement(insertNameSql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            return ps;
        };
        jdbcTemplate.update(psc, keyHolder);
        return keyHolder.getKey().intValue();
    }
}
