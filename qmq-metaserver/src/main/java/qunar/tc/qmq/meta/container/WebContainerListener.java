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

package qunar.tc.qmq.meta.container;


import qunar.tc.qmq.meta.startup.ServerWrapper;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * User: zhaohuiyu Date: 1/7/13 Time: 4:35 PM
 */
public class WebContainerListener implements ServletContextListener {
    private ServerWrapper wrapper;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        wrapper = new ServerWrapper();
        wrapper.start(sce.getServletContext());
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        if (this.wrapper != null) {
            wrapper.destroy();
        }
    }
}
