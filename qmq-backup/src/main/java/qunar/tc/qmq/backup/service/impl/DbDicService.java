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

package qunar.tc.qmq.backup.service.impl;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.backup.store.DicStore;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author yiqun.fan create on 17-10-31.
 */
public class DbDicService implements DicService {
    private static final int MAX_SIZE = 1000000;

    private final DicStore dicStore;
    private final String pattern;
    private final LoadingCache<String, String> name2IdCache;

    public DbDicService(DicStore dicStore, String pattern) {
        this.dicStore = dicStore;
        this.pattern = pattern;
        this.name2IdCache = CacheBuilder.newBuilder()
                .maximumSize(MAX_SIZE).expireAfterAccess(1, TimeUnit.DAYS)
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String key) {
                        try {
                            return getOrCreateId(key);
                        } catch (EmptyResultDataAccessException e) {
                            return "";
                        }
                    }
                });
    }

    @Override
    public String name2Id(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return "";
        }
        return name2IdCache.getUnchecked(name);
    }

    private String getOrCreateId(String name) {
        for (int i = 0; i < 5; ++i) {
            try {
                final int nameId = dicStore.getNameId(name);
                return String.format(pattern, nameId);
            } catch (EmptyResultDataAccessException e) {
                final int newNameId = ThreadLocalRandom.current().nextInt(0, 999999);
                try {
                    dicStore.insertName(newNameId, name);
                    return String.format(pattern, newNameId);
                } catch (DuplicateKeyException ex) {
                }
            }
        }
        throw new RuntimeException("重试多次仍然无法产生有效name id");
    }

}
