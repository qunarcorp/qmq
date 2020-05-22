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

package qunar.tc.qmq.meta.web;

import com.google.common.base.Strings;
import qunar.tc.qmq.meta.cache.BrokerMetaManager;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-04-02 11:58
 */
public class SlaveServerAddressSupplierServlet extends HttpServlet {

    private final BrokerMetaManager brokerMetaManager = BrokerMetaManager.getInstance();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String groupName = req.getParameter("groupName");
        if (Strings.isNullOrEmpty(groupName)) return;
        resp.setStatus(HttpServletResponse.SC_OK);
        String address = brokerMetaManager.getSlaveHttpAddress(groupName);
        resp.getWriter().print(address);
    }

}
