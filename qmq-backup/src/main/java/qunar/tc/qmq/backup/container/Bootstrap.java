package qunar.tc.qmq.backup.container;

import qunar.tc.qmq.backup.startup.ServerWrapper;
import qunar.tc.qmq.configuration.DynamicConfigLoader;

public class Bootstrap {
    public static void main(String[] args) {
        ServerWrapper wrapper = new ServerWrapper(DynamicConfigLoader.load("backup.properties"));
        Runtime.getRuntime().addShutdownHook(new Thread(wrapper::destroy));
        wrapper.start();
    }

}
