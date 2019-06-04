package qunar.tc.qmq.backup.config;

import qunar.tc.qmq.configuration.DynamicConfig;

public interface BackupConfig {
    String getBrokerGroup();

    void setBrokerGroup(String name);

    DynamicConfig getDynamicConfig();
}
