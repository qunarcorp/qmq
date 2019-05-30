package qunar.tc.qmq.backup.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.RecordQuery;
import qunar.tc.qmq.backup.base.RecordResult;
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
 * @since 2019-03-05 15:45
 */
public class MessageRecordsServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(MessageRecordsServlet.class);

    private final Serializer serializer = Serializer.getSerializer();

    private static final MessageService messageService = MessageServiceImpl.getInstance();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        RecordQuery query = getQuery(req);
        if (query == null) {
            response(resp);
            return;
        }

        try {
            RecordResult result = messageService.findRecords(query);
            response(resp, HttpServletResponse.SC_OK, serializer.serialize(result));
        } catch (Exception e) {
            LOG.error("Failed to find message records.", e);
            response(resp);
        }
    }

    private void response(HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        RecordResult result = new RecordResult(Collections.emptyList());
        resp.getWriter().println(serializer.serialize(result));
    }

    private void response(HttpServletResponse resp, int code, Object data) throws IOException {
        resp.setStatus(code);
        resp.getWriter().println(data);
    }

    private RecordQuery getQuery(HttpServletRequest req) {
        try {
            final String recordQueryStr = req.getParameter("recordQuery");
            return serializer.deSerialize(recordQueryStr, RecordQuery.class);
        } catch (Exception e) {
            LOG.error("Get record query failed.", e);
            return null;
        }
    }
}
