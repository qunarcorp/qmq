package qunar.tc.qmq.meta.management;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.meta.model.ReadonlyBrokerGroupSetting;
import qunar.tc.qmq.meta.service.ReadonlyBrokerGroupSettingService;

import javax.servlet.http.HttpServletRequest;

/**
 * @author keli.wang
 * @since 2018/7/30
 */
public class MarkReadonlyBrokerGroupAction implements MetaManagementAction {
    private static final Logger LOG = LoggerFactory.getLogger(MarkReadonlyBrokerGroupAction.class);

    private final ReadonlyBrokerGroupSettingService service;

    public MarkReadonlyBrokerGroupAction(final ReadonlyBrokerGroupSettingService service) {
        this.service = service;
    }

    @Override
    public Object handleAction(final HttpServletRequest req) {
        final String subject = req.getParameter("subject");
        final String brokerGroup = req.getParameter("brokerGroup");

        if (Strings.isNullOrEmpty(subject) || Strings.isNullOrEmpty(brokerGroup)) {
            return ActionResult.error("必须同时提供 subject 和 brokerGroup 两个参数");
        }

        try {
            service.addSetting(new ReadonlyBrokerGroupSetting(subject, brokerGroup));
            return ActionResult.ok("success");
        } catch (Exception e) {
            LOG.error("add readonly broker group setting failed. subject: {}, brokerGroup: {}", subject, brokerGroup, e);
            return ActionResult.error(e.getMessage());
        }
    }
}
