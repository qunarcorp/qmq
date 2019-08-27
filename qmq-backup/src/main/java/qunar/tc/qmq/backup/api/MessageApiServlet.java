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

package qunar.tc.qmq.backup.api;

import qunar.tc.qmq.backup.base.BackupQuery;
import qunar.tc.qmq.backup.base.MessageQueryResult;
import qunar.tc.qmq.backup.service.MessageService;
import qunar.tc.qmq.backup.util.GsonUtils;

import javax.servlet.AsyncContext;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.CompletableFuture;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019-01-17 12:08
 */
public class MessageApiServlet extends AbstractGetServlet {
    public MessageApiServlet(MessageService messageService) {
        super(messageService);
    }

    @Override
    protected void query(HttpServletRequest req, HttpServletResponse resp, BackupQuery query) {
        final AsyncContext context = req.startAsync();
        final ServletResponse response = context.getResponse();
        final CompletableFuture<MessageQueryResult> future = messageService.findMessages(query);
        future.exceptionally(throwable -> EMPTY_MESSAGE_QUERY_RESULT).thenAccept(messageQueryResult -> {
            response(response, GsonUtils.serialize(messageQueryResult));
            context.complete();
        });

    }
}
