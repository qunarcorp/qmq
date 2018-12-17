package qunar.tc.qmq.meta.store;

import qunar.tc.qmq.meta.model.ClientDbInfo;

public interface ClientDbConfigurationStore {
    void insertDb(ClientDbInfo clientDbInfo);
}
