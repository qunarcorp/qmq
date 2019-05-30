package qunar.tc.qmq.backup.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.service.MessageService;
import qunar.tc.qmq.backup.service.impl.MessageServiceImpl;
import qunar.tc.qmq.backup.util.Serializer;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-05 14:47
 */
public abstract class AbstractGetServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractGetServlet.class);

    final Serializer serializer = Serializer.getSerializer();

    static MessageService messageService = MessageServiceImpl.getInstance();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        final BackupQuery query = getQuery(req);
        if (query == null) {
            response(resp, HttpServletResponse.SC_OK, serializer.serialize(Collections.emptyList()));
            return;
        }

        query(req, resp, query);
    }

    void response(HttpServletResponse resp, int code, Object data) throws IOException {
        resp.setHeader("Content-type", "text/html;charset=UTF-8");
        resp.setCharacterEncoding("UTF-8");
        resp.setStatus(code);
        resp.getWriter().println(data);
    }

    protected abstract void query(final HttpServletRequest req, final HttpServletResponse resp, final BackupQuery query) throws IOException;

    private BackupQuery getQuery(final HttpServletRequest request) {
        try {
            final String queryStr = request.getParameter("backupQuery");
            return serializer.deSerialize(queryStr, BackupQuery.class);
        } catch (Exception e) {
            LOG.error("Get backup query error.", e);
            return null;
        }
    }
}
