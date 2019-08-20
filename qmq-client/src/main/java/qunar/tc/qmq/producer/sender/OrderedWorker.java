//package qunar.tc.qmq.producer.sender;
//
//import com.google.common.collect.Lists;
//import java.util.ArrayList;
//import java.util.concurrent.LinkedBlockingQueue;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * @author zhenwei.liu
// * @since 2019-08-20
// */
//public class OrderedWorker implements Runnable {
//
//	private static final Logger logger = LoggerFactory.getLogger(OrderedWorker.class);
//
//	private String name;
//	private Thread thread;
//	private LinkedBlockingQueue<Runnable> taskQueue;
//
//	public OrderedWorker(String name) {
//		this.name = name;
//		this.taskQueue = new LinkedBlockingQueue<>();
//	}
//
//	public void init() {
//		this.thread = new Thread(this, name);
//		this.thread.start();
//	}
//
//	public void addTask(Runnable task) {
//		taskQueue.add(task);
//	}
//
//	@Override
//	public void run() {
//		while (true) {
//			try {
//				ArrayList<Runnable> tasks = Lists.newArrayList();
//				tasks.add(taskQueue.take());
//				taskQueue.drainTo(tasks);
//
//				for (Runnable task : tasks) {
//					task.run();
//				}
//			} catch (InterruptedException e) {
//				logger.error("ordered task is interrupted, name {}", name, e);
//			} catch (Throwable t) {
//				logger.error("ordered task error, name {}", name, t);
//			}
//		}
//	}
//}
