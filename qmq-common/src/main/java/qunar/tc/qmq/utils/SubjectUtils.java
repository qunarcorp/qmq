package qunar.tc.qmq.utils;

import static com.google.common.base.CharMatcher.BREAKING_WHITESPACE;

import com.google.common.base.CharMatcher;

public class SubjectUtils {
	private static final CharMatcher ILLEGAL_MATCHER = CharMatcher.anyOf("/\r\n");

	public static boolean isInValid(String subject) {
		if (subject == null) return true;
		if (subject.length() == 0) return true;

		return ILLEGAL_MATCHER.matchesAnyOf(subject) || BREAKING_WHITESPACE.matchesAnyOf(subject);
	}
}
