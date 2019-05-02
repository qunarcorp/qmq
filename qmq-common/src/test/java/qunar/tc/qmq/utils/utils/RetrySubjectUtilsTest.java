package qunar.tc.qmq.utils.utils;

import org.junit.Test;

import static org.junit.Assert.*;
import static qunar.tc.qmq.utils.RetrySubjectUtils.*;

public class RetrySubjectUtilsTest {

    @Test
    public void testGetRealSubject() {
        assertNull(getRealSubject(null));

        assertEquals("%RETRY", getRealSubject("%RETRY"));
        assertEquals("foo", getRealSubject("%DEAD_RETRY%foo%bar"));
    }

    @Test
    public void testParseSubjectAndGroup() {
        assertNull(parseSubjectAndGroup(null));
        assertNull(parseSubjectAndGroup("%DEAD_RETRY"));

        assertArrayEquals(new String[]{"foo", "bar"}, parseSubjectAndGroup("%DEAD_RETRY%foo%bar"));
    }

    @Test
    public void testIsRealSubject() {
        assertFalse(isRealSubject(null));
        assertFalse(isRealSubject("%RETRY"));
        assertFalse(isRealSubject("%DEAD_RETRY"));

        assertTrue(isRealSubject("foo"));
    }

    @Test
    public void testBuildRetrySubject() {
        assertEquals("%RETRY%foo%baz", buildRetrySubject("foo", "baz"));
    }

    @Test
    public void testBuildDeadRetrySubject() {
        assertEquals("%DEAD_RETRY%foo%baz", buildDeadRetrySubject("foo", "baz"));
    }
}
