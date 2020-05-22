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

import picocli.CommandLine;
import qunar.tc.qmq.tools.MetaManagementService;

import java.util.HashMap;

@CommandLine.Command(name = "AddDb", mixinStandardHelpOptions = true, sortOptions = false)
public class AddDbCommand implements Runnable {
    @CommandLine.Option(names = {"--metaserver"}, required = true, description = {"meta server address, format: <host> or <host>:<port>"})
    private String metaserver;

    @CommandLine.Option(names = {"--token"}, required = true)
    private String apiToken;

    @CommandLine.Option(names = {"--type"}, required = false, defaultValue = "mmm")
    private String type;

    @CommandLine.Option(names = {"--host"}, required = true)
    private String host;

    @CommandLine.Option(names = {"--port"}, required = true)
    private int port;

    @CommandLine.Option(names = {"--username"}, required = true)
    private String userName;

    @CommandLine.Option(names = {"--password"}, required = true)
    private String password;

    @CommandLine.Option(names = {"--room"}, required = false, defaultValue = "default")
    private String room;

    private final MetaManagementService service;

    public AddDbCommand(MetaManagementService service) {
        this.service = service;
    }

    @Override
    public void run() {
        final HashMap<String, String> params = new HashMap<>();
        params.put("action", "AddDb");
        params.put("type", type);
        params.put("host", host);
        params.put("port", String.valueOf(port));
        params.put("username", userName);
        params.put("password", password);
        params.put("room", room);

        System.out.println(service.post(metaserver, apiToken, params));
    }
}
