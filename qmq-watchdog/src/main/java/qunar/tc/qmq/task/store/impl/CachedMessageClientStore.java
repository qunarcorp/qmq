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
