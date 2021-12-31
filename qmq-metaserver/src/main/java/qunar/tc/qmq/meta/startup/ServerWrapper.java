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

package qunar.tc.qmq.meta.startup;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.Disposable;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.cache.AliveClientManager;
import qunar.tc.qmq.meta.cache.BrokerMetaManager;
import qunar.tc.qmq.meta.cache.CacheManager;
import qunar.tc.qmq.meta.cache.CachedMetaInfoManager;
import qunar.tc.qmq.meta.cache.CachedOfflineStateManager;
import qunar.tc.qmq.meta.cache.MetaHeartBeatManager;
import qunar.tc.qmq.meta.loadbalance.LoadBalance;
import qunar.tc.qmq.meta.loadbalance.LoadBalanceFactory;
import qunar.tc.qmq.meta.loadbalance.RandomLoadBalance;
import qunar.tc.qmq.meta.management.AddBrokerAction;
import qunar.tc.qmq.meta.management.AddNewSubjectAction;
import qunar.tc.qmq.meta.management.AddSubjectBrokerGroupAction;
import qunar.tc.qmq.meta.management.ExtendSubjectRouteAction;
import qunar.tc.qmq.meta.management.ListBrokerGroupsAction;
import qunar.tc.qmq.meta.management.ListBrokersAction;
import qunar.tc.qmq.meta.management.ListSubjectRoutesAction;
import qunar.tc.qmq.meta.management.MarkReadonlyBrokerGroupAction;
import qunar.tc.qmq.meta.management.MetaManagementActionSupplier;
import qunar.tc.qmq.meta.management.RegisterClientDbAction;
import qunar.tc.qmq.meta.management.RemoveSubjectBrokerGroupAction;
import qunar.tc.qmq.meta.management.ReplaceBrokerAction;
import qunar.tc.qmq.meta.management.ResetOffsetAction;
import qunar.tc.qmq.meta.management.TokenVerificationAction;
import qunar.tc.qmq.meta.management.UnMarkReadonlyBrokerGroupAction;
import qunar.tc.qmq.meta.processor.BrokerAcquireMetaProcessor;
import qunar.tc.qmq.meta.processor.BrokerRegisterProcessor;
import qunar.tc.qmq.meta.processor.ClientRegisterProcessor;
import qunar.tc.qmq.meta.route.ReadonlyBrokerGroupManager;
import qunar.tc.qmq.meta.route.SubjectRouteExtendFactory;
import qunar.tc.qmq.meta.route.SubjectRouter;
import qunar.tc.qmq.meta.route.impl.DefaultSubjectRouter;
import qunar.tc.qmq.meta.route.impl.DelayRouter;
import qunar.tc.qmq.meta.route.impl.SubjectRouterWrapper;
import qunar.tc.qmq.meta.service.ReadonlyBrokerGroupSettingService;
import qunar.tc.qmq.meta.spi.cache.ClientServiceContext;
import qunar.tc.qmq.meta.spi.cache.impl.DefaultClientAliveServiceImpl;
import qunar.tc.qmq.meta.store.BrokerStore;
import qunar.tc.qmq.meta.store.ClientDbConfigurationStore;
import qunar.tc.qmq.meta.store.ReadonlyBrokerGroupSettingStore;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.meta.store.impl.BrokerStoreImpl;
import qunar.tc.qmq.meta.store.impl.ClientDbConfigurationStoreImpl;
import qunar.tc.qmq.meta.store.impl.DatabaseStore;
import qunar.tc.qmq.meta.store.impl.ReadonlyBrokerGroupSettingStoreImpl;
import qunar.tc.qmq.netty.DefaultConnectionEventHandler;
import qunar.tc.qmq.netty.NettyServer;
import qunar.tc.qmq.protocol.CommandCode;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author yunfeng.yang
 * @since 2017/8/30
 */
@SuppressWarnings("all")
public class ServerWrapper implements Disposable {
    private static final Logger LOG = LoggerFactory.getLogger(ServerWrapper.class);

    private static final int DEFAULT_META_SERVER_PORT = 20880;

    private final List<Disposable> resources;
    private final DynamicConfig config;

    private ClientServiceContext clientServiceContext = new ClientServiceContext();

    public ServerWrapper(DynamicConfig config) {
        this.resources = new ArrayList<>();
        this.config = config;
    }

    public void start(ServletContext context) {
        final int port = config.getInt("meta.server.port", DEFAULT_META_SERVER_PORT);
        context.setAttribute("port", port);

        JdbcTemplate jdbcTemplate = JdbcTemplateHolder.getOrCreate();
        final Store store = new DatabaseStore(jdbcTemplate);
        final BrokerStore brokerStore = new BrokerStoreImpl(jdbcTemplate);
        final BrokerMetaManager brokerMetaManager = BrokerMetaManager.getInstance();
        brokerMetaManager.init(brokerStore);
        final ReadonlyBrokerGroupSettingStore readonlyBrokerGroupSettingStore = new ReadonlyBrokerGroupSettingStoreImpl(jdbcTemplate);
        final CachedMetaInfoManager cachedMetaInfoManager = new CachedMetaInfoManager(config, store, readonlyBrokerGroupSettingStore);
        final SubjectRouter subjectRouter = createSubjectRouter(cachedMetaInfoManager, store);
        final ReadonlyBrokerGroupManager readonlyBrokerGroupManager = new ReadonlyBrokerGroupManager(cachedMetaInfoManager);
        CacheManager cacheManager = CacheManager.of(new MetaHeartBeatManager(store), AliveClientManager.getInstance(),cachedMetaInfoManager,
                CachedOfflineStateManager.SUPPLIER.get(),readonlyBrokerGroupManager);
        final ClientRegisterProcessor clientRegisterProcessor = new ClientRegisterProcessor(subjectRouter, cacheManager, store);
        final BrokerRegisterProcessor brokerRegisterProcessor = new BrokerRegisterProcessor(config, cachedMetaInfoManager, store);
        final BrokerAcquireMetaProcessor brokerAcquireMetaProcessor = new BrokerAcquireMetaProcessor(new BrokerStoreImpl(jdbcTemplate));
        final ReadonlyBrokerGroupSettingService readonlyBrokerGroupSettingService = new ReadonlyBrokerGroupSettingService(readonlyBrokerGroupSettingStore);

        final NettyServer metaNettyServer = new NettyServer("meta", Runtime.getRuntime().availableProcessors(), port, new DefaultConnectionEventHandler("meta"));
        metaNettyServer.registerProcessor(CommandCode.CLIENT_REGISTER, clientRegisterProcessor);
        metaNettyServer.registerProcessor(CommandCode.BROKER_REGISTER, brokerRegisterProcessor);
        metaNettyServer.registerProcessor(CommandCode.BROKER_ACQUIRE_META, brokerAcquireMetaProcessor);
        metaNettyServer.start();

        ClientDbConfigurationStore clientDbConfigurationStore = new ClientDbConfigurationStoreImpl();

        final MetaManagementActionSupplier actions = MetaManagementActionSupplier.getInstance();
        actions.register("AddBroker", new TokenVerificationAction(new AddBrokerAction(brokerStore)));
        actions.register("ReplaceBroker", new TokenVerificationAction(new ReplaceBrokerAction(brokerStore)));
        actions.register("ListBrokers", new ListBrokersAction(brokerStore));
        actions.register("ListBrokerGroups", new ListBrokerGroupsAction(store));
        actions.register("ListSubjectRoutes", new ListSubjectRoutesAction(store));
        actions.register("AddSubjectBrokerGroup", new TokenVerificationAction(new AddSubjectBrokerGroupAction(store, cachedMetaInfoManager)));
        actions.register("RemoveSubjectBrokerGroup", new TokenVerificationAction(new RemoveSubjectBrokerGroupAction(store, cachedMetaInfoManager)));
        actions.register("AddNewSubject", new TokenVerificationAction(new AddNewSubjectAction(store)));
        actions.register("ExtendSubjectRoute", new TokenVerificationAction(new ExtendSubjectRouteAction(store, cachedMetaInfoManager)));
        actions.register("AddDb", new TokenVerificationAction(new RegisterClientDbAction(clientDbConfigurationStore)));
        actions.register("MarkReadonlyBrokerGroup", new TokenVerificationAction(new MarkReadonlyBrokerGroupAction(readonlyBrokerGroupSettingService)));
        actions.register("UnMarkReadonlyBrokerGroup", new TokenVerificationAction(new UnMarkReadonlyBrokerGroupAction(readonlyBrokerGroupSettingService)));
        actions.register("ResetOffset", new TokenVerificationAction(new ResetOffsetAction(store)));

        resources.add(cachedMetaInfoManager);
        resources.add(brokerMetaManager);
        resources.add(metaNettyServer);
        registryService(cacheManager);
    }

    private void registryService(CacheManager cacheManager) {
        DefaultClientAliveServiceImpl clientAlive = new DefaultClientAliveServiceImpl(cacheManager);
        clientServiceContext.addService(clientAlive.name(), clientAlive);
    }

    private SubjectRouter createSubjectRouter(CachedMetaInfoManager cachedMetaInfoManager, Store store) {
        LoadBalance loadBalance = LoadBalanceFactory.getByName(config.getString("meta.server.load.balance", RandomLoadBalance.DEFAULT_LOAD_BALANCE));
        DelayRouter delayRouter = new DelayRouter(cachedMetaInfoManager, new DefaultSubjectRouter(config, cachedMetaInfoManager, store, loadBalance), loadBalance);
        SubjectRouteExtendFactory.registry(clientServiceContext);
        return new SubjectRouterWrapper(delayRouter);
    }


    @Override
    public void destroy() {
        if (resources.isEmpty()) return;

        for (int i = resources.size() - 1; i >= 0; --i) {
            try {
                resources.get(i).destroy();
            } catch (Throwable e) {
                LOG.error("destroy resource failed", e);
            }
        }
    }
}
