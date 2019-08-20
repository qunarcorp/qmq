//package qunar.tc.qmq.producer.sender;
//
//import com.google.common.collect.Maps;
//import java.util.Map;
//
///**
// * @author zhenwei.liu
// * @since 2019-08-20
// */
//public class OrderedWorkerManager {
//
//	private int workerNum;
//	private Map<Integer, OrderedWorker> workerMap;
//
//	public OrderedWorkerManager(int workerNum) {
//		this.workerNum = workerNum;
//		this.workerMap = Maps.newHashMap();
//		String workerName = "ordered-worker-%s";
//		for (int i = 0; i < workerNum; i++) {
//			OrderedWorker worker = new OrderedWorker(String.format(workerName, i));
//			worker.init();
//			workerMap.put(i, worker);
//		}
//	}
//
//	public OrderedWorker getWorker(Object key) {
//
//	}
//}
