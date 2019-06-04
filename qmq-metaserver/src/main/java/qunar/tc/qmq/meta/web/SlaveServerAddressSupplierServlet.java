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
