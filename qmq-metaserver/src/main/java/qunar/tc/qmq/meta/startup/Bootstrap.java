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

package qunar.tc.qmq.meta.startup;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.meta.web.MetaManagementServlet;
import qunar.tc.qmq.meta.web.MetaServerAddressSupplierServlet;
import qunar.tc.qmq.meta.web.OnOfflineServlet;
import qunar.tc.qmq.meta.web.SubjectConsumerServlet;

/**
 * @author keli.wang
 * @since 2018-12-04
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setResourceBase(System.getProperty("java.io.tmpdir"));
        DynamicConfig config = DynamicConfigLoader.load("metaserver.properties");
        final ServerWrapper wrapper = new ServerWrapper(config);
        wrapper.start(context.getServletContext());

        context.addServlet(MetaServerAddressSupplierServlet.class, "/meta/address");
        context.addServlet(MetaManagementServlet.class, "/management");
        context.addServlet(SubjectConsumerServlet.class, "/subject/consumers");
        context.addServlet(OnOfflineServlet.class, "/onoffline");

        // TODO(keli.wang): allow set port use env
        int port = config.getInt("meta.server.discover.port", 8080);
        final Server server = new Server(port);
        server.setHandler(context);
        server.start();
        server.join();
    }
}
