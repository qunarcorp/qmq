package qunar.tc.qmq.backup.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.configuration.DynamicConfig;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static qunar.tc.qmq.backup.config.DefaultBackupConfig.ACQUIRE_BACKUP_META_URL;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-02-26 16:57
 */
public class SlaveMetaSupplier {
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
                return response.getResponseBody();
            }
        } catch (Exception e) {
            LOG.error("backup meta supplier refresh error.", e);
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
}