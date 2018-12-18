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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import qunar.tc.qmq.utils.NetworkUtils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author yunfeng.yang
 * @since 2017/9/1
 */
public class MetaServerAddressSupplierServlet extends HttpServlet {
    private final String localhost;

    public MetaServerAddressSupplierServlet() {
        this.localhost = NetworkUtils.getLocalAddress();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(localhost), "get localhost error");
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.getWriter().write(buildLocalAddress());
    }

    private String buildLocalAddress() {
        final int port = (int) getServletContext().getAttribute("port");
        return localhost + ":" + port;
    }
}
