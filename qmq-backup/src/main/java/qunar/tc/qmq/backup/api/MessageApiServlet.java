package qunar.tc.qmq.backup.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.backup.base.BackupMessage;
import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.ResultIterable;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-01-17 12:08
 */
public class MessageApiServlet extends AbstractGetServlet {
    private static final Logger LOG = LoggerFactory.getLogger(MessageApiServlet.class);

    @Override
    protected void query(HttpServletRequest req, HttpServletResponse resp, BackupQuery query) throws IOException {
        try {
            ResultIterable<BackupMessage> resultIterable = messageService.findMessages(query);
            response(resp, HttpServletResponse.SC_OK, serializer.serialize(resultIterable));
        } catch (Exception e) {
            LOG.error("Failed to find messages.", e);
            response(resp, HttpServletResponse.SC_OK, serializer.serialize(Collections.emptyList()));
        }
    }

}