package qunar.tc.qmq.task.store.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.springframework.jdbc.core.JdbcTemplate;
import qunar.tc.qmq.task.database.DatasourceWrapper;

import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA. User: liuzz Date: 12-12-21 Time: 下午3:33
 */
public class CachedMessageClientStore extends MessageClientStore {
    private static final LoadingCache<DatasourceWrapper, JdbcTemplate> CACHE = CacheBuilder
            .newBuilder()
            .expireAfterAccess(1, TimeUnit.HOURS)
            .build(new CacheLoader<DatasourceWrapper, JdbcTemplate>() {
                @Override
                public JdbcTemplate load(DatasourceWrapper key) {
                    return new JdbcTemplate(key.datasource());
                }
            });

    public void invalidate(DatasourceWrapper dataSource) {
        CACHE.invalidate(dataSource);
    }

    @Override
    protected JdbcTemplate create(DatasourceWrapper dataSource) {
        return CACHE.getUnchecked(dataSource);
    }
}
