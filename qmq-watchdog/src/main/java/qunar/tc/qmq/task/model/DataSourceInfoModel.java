package qunar.tc.qmq.task.model;

import java.net.URI;
import java.util.Date;

public class DataSourceInfoModel extends DataSourceInfo {
    private static final long serialVersionUID = -1249880451242730984L;

    private int status;

    private Date updateTime;

    private String appCode;

    private String room;

    private String userName;

    private String password;

    public String getScheme() {
        return URI.create(getUrl()).getScheme();
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getAppCode() {
        return appCode;
    }

    public void setAppCode(String appCode) {
        this.appCode = appCode;
    }

    public String getRoom() {
        return room;
    }

    public void setRoom(String room) {
        this.room = room;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
