package qunar.tc.qmq.meta.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.common.JsonUtils;
import qunar.tc.qmq.common.PartitionConstants;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.BrokerGroup;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.cache.DefaultCachedMetaInfoManager;
import qunar.tc.qmq.meta.order.AveragePartitionAllocator;
import qunar.tc.qmq.meta.order.DefaultPartitionNameResolver;
import qunar.tc.qmq.meta.order.DefaultPartitionService;
import qunar.tc.qmq.meta.order.PartitionService;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.meta.route.impl.DefaultSubjectRouter;
import qunar.tc.qmq.meta.store.impl.ClientMetaInfoStoreImpl;
import qunar.tc.qmq.meta.store.impl.DatabaseStore;
import qunar.tc.qmq.meta.store.impl.PartitionAllocationStoreImpl;
import qunar.tc.qmq.meta.store.impl.PartitionSetStoreImpl;
import qunar.tc.qmq.meta.store.impl.PartitionStoreImpl;
import qunar.tc.qmq.meta.store.impl.ReadonlyBrokerGroupSettingStoreImpl;

/**
 * Partition 扩容缩容接口
 *
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class PartitionManagementServlet extends HttpServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionManagementServlet.class);
    private static final ObjectMapper jsonMapper = JsonUtils.getMapper();
    private final PartitionService partitionService;
    private final CachedMetaInfoManager cachedMetaInfoManager;
    private final SubjectRouter subjectRouter;

    public PartitionManagementServlet() {
        DatabaseStore store = new DatabaseStore();
        this.partitionService = new DefaultPartitionService(
                new DefaultPartitionNameResolver(),
                store,
                new ClientMetaInfoStoreImpl(),
                new PartitionStoreImpl(),
                new PartitionSetStoreImpl(),
                new PartitionAllocationStoreImpl(),
                new AveragePartitionAllocator(),
                JdbcTemplateHolder.getTransactionTemplate()
        );
        this.cachedMetaInfoManager = new DefaultCachedMetaInfoManager(store,
                new ReadonlyBrokerGroupSettingStoreImpl(JdbcTemplateHolder.getOrCreate()), partitionService);
        this.subjectRouter = new DefaultSubjectRouter(cachedMetaInfoManager, store);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String subject = req.getParameter("subject");
        String physicalPartitionNumStr = req.getParameter("physicalPartitionNum");
        int physicalPartitionNum = physicalPartitionNumStr == null ?
                PartitionConstants.DEFAULT_PHYSICAL_PARTITION_NUM : Integer.valueOf(physicalPartitionNumStr);

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setHeader("Content-Type", "application/json");
        PrintWriter writer = resp.getWriter();
        try {
            List<BrokerGroup> brokerGroups = subjectRouter.route(subject, ClientType.PRODUCER.getCode());
            if (CollectionUtils.isEmpty(brokerGroups)) {
                throw new IllegalArgumentException(String.format("无法找到 broker group. Subject: %s", subject));
            }
            partitionService.updatePartitions(subject, physicalPartitionNum,
                    brokerGroups.stream().map(BrokerGroup::getGroupName).collect(
                            Collectors.toList()));
            writer.println(jsonMapper.writeValueAsString(new JsonResult<>(ResultStatus.OK, "成功", null)));
        } catch (Throwable t) {
            LOGGER.error("顺序消息分配失败 {}", subject, t);
            writer.println(jsonMapper.writeValueAsString(
                    new JsonResult<>(ResultStatus.SYSTEM_ERROR, String.format("失败 %s", t.getMessage()), null)));
        }

    }
}
