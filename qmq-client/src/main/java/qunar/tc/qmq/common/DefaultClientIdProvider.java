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

package qunar.tc.qmq.common;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.utils.NetworkUtils;

import java.util.UUID;

/**
 * Created by zhaohui.yu
 * 4/2/18
 */
class DefaultClientIdProvider implements ClientIdProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ClientIdProvider.class);

    @Override
    public String get() {
        return NetworkUtils.getLocalHostname() + "@@" + defaultUniqueId();
    }

    private String defaultUniqueId() {
        final String location = getPackageLocation();
        if (Strings.isNullOrEmpty(location)) {
            return UUID.randomUUID().toString();
        }

        try {
            return Hashing.md5().hashString(location, Charsets.UTF_8).toString();
        } catch (Exception e) {
            LOG.error("compute md5sum for jar package location failed.", e);
        }

        return UUID.randomUUID().toString();
    }

    private String getPackageLocation() {
        try {
            return ClientIdProvider.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        } catch (Exception e) {
            LOG.warn("get jar package location failed.", e);
        }

        return "";
    }
}
