package qunar.tc.qmq.meta.route;

import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author yunfeng.yang
 * @since 2018/3/1
 */
public class ReadonlyBrokerGroupManager {
    private final CachedMetaInfoManager cachedMetaInfoManager;

    public ReadonlyBrokerGroupManager(final CachedMetaInfoManager cachedMetaInfoManager) {
        this.cachedMetaInfoManager = cachedMetaInfoManager;
    }

    public List<BrokerGroup> disableReadonlyBrokerGroup(String realSubject, int clientTypeCode, List<BrokerGroup> brokerGroups) {
        if (clientTypeCode != ClientType.PRODUCER.getCode()
                && clientTypeCode != ClientType.DELAY_PRODUCER.getCode()) {
            return brokerGroups;
        }

        List<BrokerGroup> result = new ArrayList<>();
        for (BrokerGroup brokerGroup : brokerGroups) {
            if (isReadonlyForSubject(realSubject, brokerGroup.getGroupName())) {
                continue;
            }

            result.add(brokerGroup);
        }

        return result;
    }

    private boolean isReadonlyForSubject(final String subject, final String brokerGroup) {
        final Set<String> subjects = cachedMetaInfoManager.getBrokerGroupReadonlySubjects(brokerGroup);
        if (subjects == null) {
            return false;
        }

        return subjects.contains(subject) || subjects.contains("*");
    }
}
