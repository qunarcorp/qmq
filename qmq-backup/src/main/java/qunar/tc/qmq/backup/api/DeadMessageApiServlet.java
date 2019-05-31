package qunar.tc.qmq.backup.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.MessageQueryResult;
import qunar.tc.qmq.backup.service.MessageService;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-11 11:29
 */
public class DeadMessageApiServlet extends AbstractGetServlet {
    private static final Logger LOG = LoggerFactory.getLogger(DeadMessageApiServlet.class);

    public DeadMessageApiServlet(MessageService messageService) {
        super(messageService);
    }

    @Override
    protected void query(HttpServletRequest req, HttpServletResponse resp, BackupQuery query) throws IOException {
        try {
            MessageQueryResult messageQueryResult = messageService.findDeadMessages(query);
            response(resp, HttpServletResponse.SC_OK, serializer.serialize(messageQueryResult));
        } catch (Exception e) {
            LOG.error("Failed to find dead messages.", e);
            response(resp, HttpServletResponse.SC_OK, serializer.serialize(Collections.emptyList()));
        }
    }
}
