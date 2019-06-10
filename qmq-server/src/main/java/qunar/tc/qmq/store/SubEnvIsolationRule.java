package qunar.tc.qmq.store;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author keli.wang
 * @since 2019-01-04
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SubEnvIsolationRule {
    private String subject;
    private String subjectEnv;
    private String subjectSubEnv;

    private String group;
    private String groupEnv;
    private String groupSubEnv;

    public String getSubject() {
        return subject;
    }

    public void setSubject(final String subject) {
        this.subject = subject;
    }

    public String getSubjectEnv() {
        return subjectEnv;
    }

    public void setSubjectEnv(final String subjectEnv) {
        this.subjectEnv = subjectEnv;
    }

    public String getSubjectSubEnv() {
        return subjectSubEnv;
    }

    public void setSubjectSubEnv(final String subjectSubEnv) {
        this.subjectSubEnv = subjectSubEnv;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(final String group) {
        this.group = group;
    }

    public String getGroupEnv() {
        return groupEnv;
    }

    public void setGroupEnv(final String groupEnv) {
        this.groupEnv = groupEnv;
    }

    public String getGroupSubEnv() {
        return groupSubEnv;
    }

    public void setGroupSubEnv(final String groupSubEnv) {
        this.groupSubEnv = groupSubEnv;
    }

    @Override
    public String toString() {
        return "SubEnvIsolationRule{" +
                "subject='" + subject + '\'' +
                ", subjectEnv='" + subjectEnv + '\'' +
                ", subjectSubEnv='" + subjectSubEnv + '\'' +
                ", group='" + group + '\'' +
                ", groupEnv='" + groupEnv + '\'' +
                ", groupSubEnv='" + groupSubEnv + '\'' +
                '}';
    }
}
