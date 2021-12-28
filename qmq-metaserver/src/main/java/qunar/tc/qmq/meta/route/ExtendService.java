package qunar.tc.qmq.meta.route;

/**
 * The interface Extend service.
 *
 * @param <T>  the type parameter
 * @param <R>  the type parameter
 * @description：
 * @author  ：zhixin.zhang
 * @date  ：Created in 下午9:30 2021/12/23
 */
public interface ExtendService<T,R> {


	/**
	 * Match list.
	 *
	 * @param request the request
	 * @return the list
	 */
	boolean match(T request);

	/**
	 * Route extend list.
	 *
	 * @param request the request
	 * @return the list
	 */
	 R extend(T request);
}
