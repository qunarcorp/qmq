package qunar.tc.qmq.backup.api;

import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.MessageQueryResult;
import qunar.tc.qmq.backup.service.MessageService;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.CompletableFuture;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-03-11 11:29
 */
public class DeadMessageApiServlet extends AbstractGetServlet {
    public DeadMessageApiServlet(MessageService messageService) {
        super(messageService);
    }

    @Override
    protected void query(HttpServletRequest req, HttpServletResponse resp, BackupQuery query) {
        final AsyncContext context = req.startAsync();
        final CompletableFuture<MessageQueryResult> future = messageService.findDeadMessages(query);
        future.exceptionally(throwable -> EMPTY_MESSAGE_QUERY_RESULT).thenAccept(messageQueryResult -> {
            response(resp, serializer.serialize(messageQueryResult));
            context.complete();
        });

    }
}
