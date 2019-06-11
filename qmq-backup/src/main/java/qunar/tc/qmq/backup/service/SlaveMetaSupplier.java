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

package qunar.tc.qmq.backup.service;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.ACQUIRE_BACKUP_META_URL;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-02-26 16:57
 */
public class SlaveMetaSupplier implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(SlaveMetaSupplier.class);

    private final String serverMetaAcquiredUrl;

    private static final AsyncHttpClient HTTP_CLIENT = new AsyncHttpClient();

    private final LoadingCache<String, String> CACHE = CacheBuilder.newBuilder().maximumSize(128).expireAfterAccess(1, TimeUnit.MINUTES).build(new CacheLoader<String, String>() {
        @Override
        public String load(String key) {
            return getServerAddress(key);
        }
    });

    private String getServerAddress(String groupName) {
        try {
            Response response = HTTP_CLIENT.prepareGet(serverMetaAcquiredUrl).addQueryParam("groupName", groupName).execute().get();
            if (response.getStatusCode() == HttpResponseStatus.OK.code()) {
                String address = response.getResponseBody();
                if (!Strings.isNullOrEmpty(address)) {
                    return address;
                }
            }
        } catch (Exception e) {
            LOG.error("Get server address error.", e);
        }
        return null;
    }


    public SlaveMetaSupplier(DynamicConfig config) {
        this.serverMetaAcquiredUrl = config.getString(ACQUIRE_BACKUP_META_URL);
    }

    public String resolveServerAddress(String brokerGroup) {
        try {
            return CACHE.get(brokerGroup);
        } catch (ExecutionException e) {
            return null;
        }
    }

    @Override
    public void destroy() {
        HTTP_CLIENT.close();
    }
}
