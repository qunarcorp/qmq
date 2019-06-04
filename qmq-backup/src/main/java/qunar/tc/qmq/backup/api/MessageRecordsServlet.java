package qunar.tc.qmq.backup.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.RecordQuery;
import qunar.tc.qmq.backup.base.RecordQueryResult;
import qunar.tc.qmq.backup.service.MessageService;
import qunar.tc.qmq.backup.util.Serializer;

import javax.servlet.AsyncContext;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-05 15:45
 */
public class MessageRecordsServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(MessageRecordsServlet.class);

    private static final Serializer serializer = Serializer.getSerializer();

    private static final RecordQueryResult EMPTY_RECORD_QUERY_RESULT = new RecordQueryResult(Collections.emptyList());

    private final MessageService messageService;

    public MessageRecordsServlet(MessageService messageService) {
        this.messageService = messageService;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        RecordQuery query = deserialize(req);
        if (query == null) {
            response(resp);
            return;
        }

        final AsyncContext context = req.startAsync();
        CompletableFuture<RecordQueryResult> future = messageService.findRecords(query);
        future.exceptionally(throwable -> EMPTY_RECORD_QUERY_RESULT).thenAccept(result -> {
            response(resp, serializer.serialize(result));
            context.complete();
        });
    }

    private void response(ServletResponse resp) throws IOException {
        resp.setContentType("application/json");
        RecordQueryResult result = new RecordQueryResult(Collections.emptyList());
        resp.getWriter().println(serializer.serialize(result));
    }

    private void response(ServletResponse resp, Object data) {
        try {
            resp.getWriter().println(data);
        } catch (IOException e) {
            LOG.error("An IOException occurred.", e);
        }
    }

    private RecordQuery deserialize(HttpServletRequest req) {
        try {
            final String recordQueryStr = req.getParameter("recordQuery");
            return serializer.deSerialize(recordQueryStr, RecordQuery.class);
        } catch (Exception e) {
            LOG.error("Get record query failed.", e);
            return null;
        }
    }
}
