package qunar.tc.qmq.meta.management;

import qunar.tc.qmq.meta.model.ClientDbInfo;
import qunar.tc.qmq.meta.store.ClientDbConfigurationStore;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;

public class RegisterClientDbAction implements MetaManagementAction {

    private final ClientDbConfigurationStore store;

    public RegisterClientDbAction(ClientDbConfigurationStore store) {
        this.store = store;
    }

    @Override
    public Object handleAction(HttpServletRequest req) {
        String type = req.getParameter("type");
        String host = req.getParameter("host");
        int port = Integer.valueOf(req.getParameter("port"));
        String userName = req.getParameter("username");
        String password = req.getParameter("password");

        String room = req.getParameter("room");
        if (room == null || room.trim().length() == 0) {
            room = "default";
        }

        String url = type + "://" + host + ":" + port;
        ClientDbInfo clientDbInfo = new ClientDbInfo();
        clientDbInfo.setUrl(url);
        clientDbInfo.setUserName(userName);
        clientDbInfo.setPassword(password);
        clientDbInfo.setRoom(room);
        try {
            store.insertDb(clientDbInfo);
            return ActionResult.ok("register success");
        } catch (Exception e) {
            return ActionResult.error("register failed " + e.getMessage());
        }
    }
}
