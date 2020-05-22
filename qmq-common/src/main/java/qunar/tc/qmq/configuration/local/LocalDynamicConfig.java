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

package qunar.tc.qmq.configuration.local;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.Listener;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author keli.wang
 * @since 2018-11-23
 */
public class LocalDynamicConfig implements DynamicConfig {
    private static final Logger LOG = LoggerFactory.getLogger(LocalDynamicConfig.class);

    private final String name;
    private final CopyOnWriteArrayList<Listener> listeners;
    private volatile File file;
    private volatile boolean loaded = false;
    private volatile Map<String, String> config;

    private final String confDir;

    LocalDynamicConfig(String name, boolean failOnNotExist) {
        this.name = name;
        this.listeners = new CopyOnWriteArrayList<>();
        this.config = new HashMap<>();
        this.confDir = System.getProperty("qmq.conf");
        this.file = getFileByName(name);

        if (failOnNotExist && (file == null || !file.exists())) {
            throw new RuntimeException("cannot find config file " + name);
        }
    }

    private File getFileByName(final String name) {
        if (confDir != null && confDir.length() > 0) {
            return new File(confDir, name);
        }
        try {
            final URL res = this.getClass().getClassLoader().getResource(name);
            if (res == null) {
                return null;
            }
            return Paths.get(res.toURI()).toFile();
        } catch (URISyntaxException e) {
            throw new RuntimeException("load config file failed", e);
        }
    }

    long getLastModified() {
        if (file == null) {
            file = getFileByName(name);
        }

        if (file == null) {
            return 0;
        } else {
            return file.lastModified();
        }
    }

    synchronized void onConfigModified() {
        if (file == null) {
            return;
        }

        loadConfig();
        executeListeners();
        loaded = true;
    }

    private void loadConfig() {
        try {
            final Properties p = new Properties();
            try (Reader reader = new BufferedReader(new FileReader(file))) {
                p.load(reader);
            }
            final Map<String, String> map = new LinkedHashMap<>(p.size());
            for (String key : p.stringPropertyNames()) {
                map.put(key, tryTrim(p.getProperty(key)));
            }

            config = Collections.unmodifiableMap(map);
        } catch (IOException e) {
            LOG.error("load local config failed. config: {}", file.getAbsolutePath(), e);
        }
    }

    private String tryTrim(String data) {
        if (data == null) {
            return null;
        } else {
            return data.trim();
        }
    }

    private void executeListeners() {
        for (Listener listener : listeners) {
            executeListener(listener);
        }
    }

    @Override
    public void addListener(Listener listener) {
        if (loaded) {
            executeListener(listener);
        }
        listeners.add(listener);
    }

    private void executeListener(Listener listener) {
        try {
            listener.onLoad(this);
        } catch (Throwable e) {
            LOG.error("trigger config listener failed. config: {}", name, e);
        }
    }

    @Override
    public String getString(String name) {
        return getValueWithCheck(name);
    }

    @Override
    public String getString(String name, String defaultValue) {
        String value = getValue(name);
        if (isBlank(value))
            return defaultValue;
        return value;
    }

    @Override
    public int getInt(String name) {
        return Integer.valueOf(getValueWithCheck(name));
    }

    @Override
    public int getInt(String name, int defaultValue) {
        String value = getValue(name);
        if (isBlank(value))
            return defaultValue;
        return Integer.valueOf(value);
    }

    @Override
    public long getLong(String name) {
        return Long.valueOf(getValueWithCheck(name));
    }

    @Override
    public long getLong(String name, long defaultValue) {
        String value = getValue(name);
        if (isBlank(value))
            return defaultValue;
        return Long.valueOf(value);
    }

    @Override
    public double getDouble(final String name) {
        return Double.valueOf(getValueWithCheck(name));
    }

    @Override
    public double getDouble(final String name, final double defaultValue) {
        String value = getValue(name);
        if (isBlank(value))
            return defaultValue;
        return Double.valueOf(value);
    }

    @Override
    public boolean getBoolean(String name, boolean defaultValue) {
        String value = getValue(name);
        if (isBlank(value))
            return defaultValue;
        return Boolean.valueOf(value);
    }


    private String getValueWithCheck(String name) {
        String value = getValue(name);
        if (isBlank(value)) {
            throw new RuntimeException("配置项: " + name + " 值为空");
        } else {
            return value;
        }
    }

    private String getValue(String name) {
        return config.get(name);
    }

    private boolean isBlank(final String s) {
        if (s == null || s.isEmpty()) {
            return true;
        }

        for (int i = 0; i < s.length(); i++) {
            if (!Character.isWhitespace(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean exist(String name) {
        return config.containsKey(name);
    }

    @Override
    public Map<String, String> asMap() {
        return new HashMap<>(config);
    }

    @Override
    public String toString() {
        return "LocalDynamicConfig{" +
                "name='" + name + '\'' +
                '}';
    }
}
