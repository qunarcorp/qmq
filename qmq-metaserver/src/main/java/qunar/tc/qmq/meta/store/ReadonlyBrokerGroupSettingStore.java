package qunar.tc.qmq.meta.store;

import qunar.tc.qmq.meta.model.ReadonlyBrokerGroupSetting;

import java.util.List;

/**
 * @author keli.wang
 * @since 2018/7/30
 */
public interface ReadonlyBrokerGroupSettingStore {
    int insert(final ReadonlyBrokerGroupSetting setting);

    int delete(final ReadonlyBrokerGroupSetting setting);

    List<ReadonlyBrokerGroupSetting> allReadonlyBrokerGroupSettings();
}
