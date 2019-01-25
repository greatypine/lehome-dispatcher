package cn.lehome.dispatcher.queue.bean;

import java.io.Serializable;

public class MasterApprenticeCacheBean implements Serializable {

    private static final long serialVersionUID = 8022368800909403201L;

    private Long lastApprenticeId;

    private Integer inviteCount;

    private long lastInviteTime;

    public Long getLastApprenticeId() {
        return lastApprenticeId;
    }

    public void setLastApprenticeId(Long lastApprenticeId) {
        this.lastApprenticeId = lastApprenticeId;
    }

    public long getLastInviteTime() {
        return lastInviteTime;
    }

    public void setLastInviteTime(long lastInviteTime) {
        this.lastInviteTime = lastInviteTime;
    }

    public Integer getInviteCount() {
        return inviteCount;
    }

    public void setInviteCount(Integer inviteCount) {
        this.inviteCount = inviteCount;
    }
}
