package qunar.tc.qmq.meta.cache;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.ClientType;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.store.Store;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午8:24 2021/12/24
 */
public class MetaHeartBeatManager{

	private static final Logger LOGGER = LoggerFactory.getLogger(MetaHeartBeatManager.class);

	private static final ScheduledExecutorService EXECUTOR = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("meta-heart-beat"));

	private final Store store;

	private final int handleLength = 400;

	private final int handleTime = 20000;

	private ArrayBlockingQueue<MetaInfoRequest> queue = new ArrayBlockingQueue(10000);

	public MetaHeartBeatManager(Store store) {
		this.store = store;
		start();
	}

	/**
	 * 单线程批量提交。
	 */
	private void start() {
		DataListener listener = new DataListener();
		new Thread(listener, "metaHeartBeatManager").start();
	}


	public void addHeartBeat(MetaInfoRequest request) {
		try {
			queue.add(request);
		} catch (Exception e) {
			LOGGER.warn("addHeartBeat error", e);
		}
	}


	private void batchUpdateHeartBeat(List<MetaInfoRequest> list) {
		if (!CollectionUtils.isEmpty(list)) {
			Map<String, List<MetaInfoRequest>> clientMap = list.stream().filter(s -> StringUtils.hasText(s.getClientId()))
					.filter(s -> s.getClientTypeCode() == ClientType.CONSUMER.getCode())
					.collect(Collectors.groupingBy(MetaInfoRequest::getClientId));
			if (clientMap != null && clientMap.size() > 0) {
				for (Map.Entry<String, List<MetaInfoRequest>> entry : clientMap.entrySet()) {
					List<String> subjects = entry.getValue().stream().filter(s -> StringUtils.hasText(s.getSubject()))
							.map(MetaInfoRequest::getSubject).collect(Collectors.toList());
					List<ClientMetaInfo> clientMetaInfos = store.queryConsumerByIdAndSubject(entry.getKey(), subjects);
					if (!CollectionUtils.isEmpty(clientMetaInfos)) {
						List<Integer> ids = clientMetaInfos.stream().filter(s -> s.getId() > 0).map(ClientMetaInfo::getId).collect(Collectors.toList());
						if (!CollectionUtils.isEmpty(ids)) {
							store.updateTimeByIds(ids);
						}
					}
				}
			}
		}
	}

	class DataListener implements Runnable {
		@Override
		public void run() {
			long startTime = System.currentTimeMillis();
			while (true) {
				try {
					int size = queue.size();
					if (size >= handleLength) {
						List<MetaInfoRequest> list = Lists.newArrayListWithCapacity(size);
						int drained = queue.drainTo(list, size);
						if (drained > 0) {
							batchUpdateHeartBeat(list);
						}
						startTime = System.currentTimeMillis();
						continue;
					}
					long currentTime = System.currentTimeMillis();
					if (currentTime - startTime > handleTime) {
						size = queue.size();
						List<MetaInfoRequest> list = Lists.newArrayListWithCapacity(size);
						int drained = queue.drainTo(list, size);
						if (drained > 0) {
							batchUpdateHeartBeat(list);
						}
						startTime = System.currentTimeMillis();
					}
				} catch (Exception e) {
					LOGGER.warn("MetaHeartBeatManager error", e);
				}
			}
		}
	}

}
