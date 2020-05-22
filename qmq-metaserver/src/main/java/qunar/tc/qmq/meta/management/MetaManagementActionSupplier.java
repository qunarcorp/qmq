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

package qunar.tc.qmq.meta.management;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author keli.wang
 * @since 2017/10/20
 */
public class MetaManagementActionSupplier {
    private static final MetaManagementActionSupplier INSTANCE = new MetaManagementActionSupplier();
    private final ConcurrentHashMap<String, MetaManagementAction> actions;

    private MetaManagementActionSupplier() {
        this.actions = new ConcurrentHashMap<>();
    }

    public static MetaManagementActionSupplier getInstance() {
        return INSTANCE;
    }

    public boolean register(final String name, final MetaManagementAction action) {
        return actions.putIfAbsent(name, action) == null;
    }

    public MetaManagementAction getAction(final String name) {
        return actions.get(name);
    }
}
