package qunar.tc.qmq.gateway.startup;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.gateway.servlet.PullServlet;
import qunar.tc.qmq.gateway.servlet.SendServlet;

/**
 * Created by zhaohui.yu
 * 12/20/18
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setResourceBase(System.getProperty("java.io.tmpdir"));
        DynamicConfig config = DynamicConfigLoader.load("gateway.properties");

        context.addServlet(PullServlet.class, "/pull/*");
        context.addServlet(SendServlet.class, "/send/*");

        int port = config.getInt("gateway.port", 8080);
        final Server server = new Server(port);
        server.setHandler(context);
        server.start();
        server.join();
    }
}
