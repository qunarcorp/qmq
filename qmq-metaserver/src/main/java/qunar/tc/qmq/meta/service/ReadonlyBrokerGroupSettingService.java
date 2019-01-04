package qunar.tc.qmq.meta.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import qunar.tc.qmq.meta.model.ReadonlyBrokerGroupSetting;
import qunar.tc.qmq.meta.store.ReadonlyBrokerGroupSettingStore;

import java.util.List;

/**
 * @author keli.wang
 * @since 2018/7/30
 */
public class ReadonlyBrokerGroupSettingService {
    private static final Logger LOG = LoggerFactory.getLogger(ReadonlyBrokerGroupSettingService.class);

    private final ReadonlyBrokerGroupSettingStore store;

    public ReadonlyBrokerGroupSettingService(final ReadonlyBrokerGroupSettingStore store) {
        this.store = store;
    }

    public void addSetting(final ReadonlyBrokerGroupSetting setting) {
        try {
            store.insert(setting);
        } catch (DuplicateKeyException e) {
            LOG.info("duplicate readonly broker group setting. setting: {}", setting);
        } catch (DataAccessException e) {
            throw new RuntimeException("database failed during save readonly broker group setting", e);
        }
    }

    public void removeSetting(final ReadonlyBrokerGroupSetting setting) {
        final int affectedRows = store.delete(setting);
        LOG.info("remove readonly broker group setting. setting: {}, affectedRows: {}", setting, affectedRows);
    }
}
