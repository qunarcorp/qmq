package qunar.tc.qmq.utils.utils;

import org.junit.Test;

import static org.junit.Assert.*;
import static qunar.tc.qmq.utils.RetryPartitionUtils.*;

public class RetryPartitionUtilsTest {

    @Test
    public void testParseSubjectAndGroup() {
        assertNull(parseSubjectAndGroup(null));
        assertNull(parseSubjectAndGroup("%DEAD_RETRY"));

        assertArrayEquals(new String[]{"foo", "bar"}, parseSubjectAndGroup("%DEAD_RETRY%foo%bar"));
    }

    @Test
    public void testIsRealSubject() {
        assertFalse(isRealPartitionName(null));
        assertFalse(isRealPartitionName("%RETRY"));
        assertFalse(isRealPartitionName("%DEAD_RETRY"));

        assertTrue(isRealPartitionName("foo"));
    }

    @Test
    public void testBuildRetrySubject() {
        assertEquals("%RETRY%foo%baz", buildRetryPartitionName("foo", "baz"));
    }

    @Test
    public void testBuildDeadRetrySubject() {
        assertEquals("%DEAD_RETRY%foo%baz", buildRetryPartitionName("foo", "baz"));
    }
}
