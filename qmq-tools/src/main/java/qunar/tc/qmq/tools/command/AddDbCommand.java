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
