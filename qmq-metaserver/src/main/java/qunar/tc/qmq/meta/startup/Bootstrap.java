/*
 * Copyright 2018 Qunar
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
 * limitations under the License.com.qunar.pay.trade.api.card.service.usercard.UserCardQueryFacade
 */

package qunar.tc.qmq.meta.startup;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import qunar.tc.qmq.meta.web.*;

/**
 * @author keli.wang
 * @since 2018-12-04
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        context.setResourceBase(System.getProperty("java.io.tmpdir"));

        final ServerWrapper wrapper = new ServerWrapper();
        wrapper.start(context.getServletContext());

        context.addServlet(MetaServerAddressSupplierServlet.class, "/meta/address");
        context.addServlet(MetaManagementServlet.class, "/management");
        context.addServlet(SubjectConsumerServlet.class, "/subject/consumers");
        context.addServlet(OnOfflineServlet.class, "/onoffline");

        // TODO(keli.wang): allow set port use env
        final Server server = new Server(8080);
        server.setHandler(context);
        server.start();
        server.join();
    }
}
