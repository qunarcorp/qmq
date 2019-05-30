package qunar.tc.qmq.backup.api;

import qunar.tc.qmq.backup.base.BackupMessage;
import qunar.tc.qmq.backup.base.BackupQuery;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-05 14:44
 */
public class MessageDetailsServlet extends AbstractGetServlet {

    @Override
    protected void query(HttpServletRequest req, HttpServletResponse resp, BackupQuery query) throws IOException {
        BackupMessage message = messageService.findMessage(query);
        response(resp, HttpServletResponse.SC_OK, serializer.serialize(message));
    }
}
