package qunar.tc.qmq.task.database;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: liuzz
 * Date: 13-1-8
 * Time: 上午11:52
 */
public class DatabaseDriverMapping {

    private final Map<String, IDatabaseDriver> map;

    public DatabaseDriverMapping(Map<String, IDatabaseDriver> map) {
        this.map = map;
    }

    public IDatabaseDriver getDatabaseMapping(String protocol) {
        return map.get(protocol);
    }
}
