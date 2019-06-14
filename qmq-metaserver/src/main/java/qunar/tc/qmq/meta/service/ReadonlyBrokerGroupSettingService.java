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

package qunar.tc.qmq.meta.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import qunar.tc.qmq.meta.model.ReadonlyBrokerGroupSetting;
import qunar.tc.qmq.meta.store.ReadonlyBrokerGroupSettingStore;

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
