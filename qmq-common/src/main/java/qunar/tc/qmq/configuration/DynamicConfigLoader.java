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

package qunar.tc.qmq.configuration;

import qunar.tc.qmq.configuration.local.LocalDynamicConfigFactory;

import java.util.ServiceLoader;

/**
 * @author keli.wang
 * @since 2018-11-23
 */
public final class DynamicConfigLoader {
    // TODO(keli.wang): can we set this using config?
    private static final DynamicConfigFactory FACTORY;

    static {
        ServiceLoader<DynamicConfigFactory> factories = ServiceLoader.load(DynamicConfigFactory.class);
        DynamicConfigFactory instance = null;
        for (DynamicConfigFactory factory : factories) {
            instance = factory;
            break;
        }

        if (instance == null) {
            instance = new LocalDynamicConfigFactory();
        }

        FACTORY = instance;
    }

    private DynamicConfigLoader() {
    }

    public static DynamicConfig load(final String name) {
        return load(name, true);
    }

    public static DynamicConfig load(final String name, final boolean failOnNotExist) {
        return FACTORY.create(name, failOnNotExist);
    }
}
