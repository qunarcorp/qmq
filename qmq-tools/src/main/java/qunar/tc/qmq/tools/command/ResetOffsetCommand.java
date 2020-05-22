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

package qunar.tc.qmq.tools.command;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import qunar.tc.qmq.tools.MetaManagementService;

import java.util.HashMap;

/**
 * @author keli.wang
 * @since 2018-12-05
 */
@Command(name = "ResetOffset", mixinStandardHelpOptions = true, sortOptions = false)
public class ResetOffsetCommand implements Runnable {
    private final MetaManagementService service;

    @Option(names = {"--metaserver"}, required = true, description = {"meta server address, format: <host> or <host>:<port>"})
    private String metaserver;

    @Option(names = {"--token"}, required = true)
    private String apiToken;

    @Option(names = {"--subject"}, required = true)
    private String subject;

    @Option(names = {"--group"}, required = true)
    private String group;

    @Option(names = {"--action"}, required = true, defaultValue = "LATEST=1, EARLIEST=2")
    private int action;

    public ResetOffsetCommand(final MetaManagementService service) {
        this.service = service;
    }

    @Override
    public void run() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("action", "ResetOffset");
        params.put("subject", subject);
        params.put("group", group);
        params.put("code", Integer.toString(action));

        System.out.println(service.post(metaserver, apiToken, params));
    }
}
