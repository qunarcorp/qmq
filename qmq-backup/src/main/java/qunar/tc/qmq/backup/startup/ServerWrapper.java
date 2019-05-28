package qunar.tc.qmq.backup.startup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;

public class ServerWrapper implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(ServerWrapper.class);

    private final DynamicConfig config;

    public ServerWrapper(DynamicConfig config) {
        this.config = config;
    }

    @Override
    public void destroy() {

    }
}
