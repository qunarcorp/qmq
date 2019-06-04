package qunar.tc.qmq.backup.store;

/**
 * @author yiqun.fan create on 17-10-31.
 */
public interface DicStore {
    String getName(int id);

    int getId(String name);

    int insertName(String name);
}
