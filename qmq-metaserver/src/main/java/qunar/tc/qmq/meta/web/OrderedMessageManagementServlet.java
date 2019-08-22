package qunar.tc.qmq.meta.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.JsonUtils;
import qunar.tc.qmq.meta.PartitionInfo;
import qunar.tc.qmq.meta.order.DefaultOrderedMessageService;
import qunar.tc.qmq.meta.order.OrderedMessageConfig;
import qunar.tc.qmq.meta.order.OrderedMessageService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class OrderedMessageManagementServlet extends HttpServlet {

    private static final Logger logger = LoggerFactory.getLogger(OrderedMessageManagementServlet.class);
    private static final ObjectMapper jsonMapper = JsonUtils.getMapper();
    private static final OrderedMessageService orderedMessageService = new DefaultOrderedMessageService();

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String subject = req.getParameter("subject");
        String physicalPartitionNumStr = req.getParameter("physicalPartitionNum");
        int physicalPartitionNum = physicalPartitionNumStr == null ?
                OrderedMessageConfig.getDefaultPhysicalPartitionNum() : Integer.valueOf(physicalPartitionNumStr);

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setHeader("Content-Type", "application/json");
        PrintWriter writer = resp.getWriter();
        try {
            PartitionInfo partitionInfo = orderedMessageService.registerOrderedMessage(subject, physicalPartitionNum);
            writer.println(jsonMapper.writeValueAsString(new JsonResult<>(ResultStatus.OK, "成功", partitionInfo)));
        } catch (Throwable t) {
            logger.error("顺序消息分配失败 {}", subject, t);
            writer.println(jsonMapper.writeValueAsString(new JsonResult<>(ResultStatus.SYSTEM_ERROR, "失败", null)));
        }

    }
}
