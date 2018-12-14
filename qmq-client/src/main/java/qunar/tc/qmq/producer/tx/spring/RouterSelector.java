package qunar.tc.qmq.producer.tx.spring;

import javax.sql.DataSource;

public interface RouterSelector {
    Object getRouteKey(DataSource dataSource);

    void setRouteKey(Object key, DataSource dataSource);

    void clearRoute(Object key, DataSource dataSource);
}
