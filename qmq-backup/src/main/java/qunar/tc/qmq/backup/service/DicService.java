package qunar.tc.qmq.backup.service;

/**
 * @author yiqun.fan create on 17-10-31.
 */
public interface DicService {
    String SIX_DIGIT_FORMAT_PATTERN = "%06d";

    String MAX_CONSUMER_GROUP_ID = "999999";

    String MIN_CONSUMER_GROUP_ID = "000000";

    String name2Id(String name);

}
