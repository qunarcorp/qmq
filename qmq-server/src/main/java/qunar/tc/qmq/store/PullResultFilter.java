package qunar.tc.qmq.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.TagType;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.protocol.consumer.PullFilter;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.consumer.SubEnvIsolationPullFilter;
import qunar.tc.qmq.protocol.consumer.TagPullFilter;
import qunar.tc.qmq.store.buffer.Buffer;

import java.util.List;

/**
 * @author keli.wang
 * @since 2019-01-02
 */
class PullResultFilter {
    private static final Logger LOG = LoggerFactory.getLogger(PullResultFilter.class);
    private final boolean enableSubEnvIsolation;
    private final SubEnvIsolationMatcher subEnvIsolationMatcher;

    PullResultFilter(final DynamicConfig config) {
        this.enableSubEnvIsolation = config.getBoolean("sub_env_isolation_filter.enable", false);
        if (this.enableSubEnvIsolation) {
            final String rulesUrl = config.getString("sub_env_isolation_filter.rules_url");
            this.subEnvIsolationMatcher = new SubEnvIsolationMatcher(rulesUrl);
            LOG.info("sub env isolation filter enabled. rules url: {}", rulesUrl);
        } else {
            this.subEnvIsolationMatcher = null;
        }
    }

    boolean needKeep(final PullRequest request, final Buffer result) {
        final List<PullFilter> filters = request.getFilters();
        if (filters == null || filters.isEmpty()) {
            return true;
        }

        for (final PullFilter filter : filters) {
            if (needDrop(request, filter, result)) {
                return false;
            }
        }
        return true;
    }

    private boolean needDrop(final PullRequest request, final PullFilter filter, final Buffer result) {
        switch (filter.type()) {
            case TAG:
                return isDropByTag((TagPullFilter) filter, result);
            case SUB_ENV_ISOLATION:
                return isDropByEnvIsolation(request, (SubEnvIsolationPullFilter) filter, result);
            default:
                throw new RuntimeException("unknown pull filter type " + filter.type());
        }
    }

    private boolean isDropByTag(final TagPullFilter filter, final Buffer result) {
        if (noRequestTag(filter)) {
            return false;
        }

        return !Tags.match(result, filter.getTags(), filter.getTagTypeCode());
    }

    private boolean noRequestTag(final TagPullFilter filter) {
        int tagTypeCode = filter.getTagTypeCode();
        if (TagType.NO_TAG.getCode() == tagTypeCode) return true;
        List<byte[]> tags = filter.getTags();
        return tags == null || tags.isEmpty();
    }

    private boolean isDropByEnvIsolation(final PullRequest request, final SubEnvIsolationPullFilter filter, final Buffer result) {
        if (!enableSubEnvIsolation) {
            return false;
        }

        return !subEnvIsolationMatcher.match(filter, request, result);
    }
}
