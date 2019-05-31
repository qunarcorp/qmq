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

package qunar.tc.qmq.container;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.startup.ServerWrapper;
import qunar.tc.qmq.web.QueryMessageServlet;

public class Bootstrap {
    public static void main(String[] args) throws Exception {
        DynamicConfig config = DynamicConfigLoader.load("broker.properties");
        ServerWrapper wrapper = new ServerWrapper(config);
        Runtime.getRuntime().addShutdownHook(new Thread(wrapper::destroy));
        wrapper.start();

        if (wrapper.isSlave()) {
            final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
            context.setContextPath("/");
            context.setResourceBase(System.getProperty("java.io.tmpdir"));
            final QueryMessageServlet servlet = new QueryMessageServlet(config, wrapper.getStorage());

            ServletHolder servletHolder = new ServletHolder(servlet);
            servletHolder.setAsyncSupported(true);
            context.addServlet(servletHolder, "/api/broker/message");

            final int port = config.getInt("slave.server.http.port", 8080);
            final Server server = new Server(port);
            server.setHandler(context);
            server.start();
            server.join();
        }
    }
}
