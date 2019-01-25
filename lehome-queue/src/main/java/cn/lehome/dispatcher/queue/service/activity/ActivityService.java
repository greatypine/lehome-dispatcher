package cn.lehome.dispatcher.queue.service.activity;

public interface ActivityService {

    String MASTER_APPRENTICE_CACHE_KEY = "master.apprentice.info:";
    String LAST_APPRENTICE_ID = "lastApprenticeId";
    String INVITE_COUNT = "inviteCount";
    String LAST_INVITE_DATE = "lastInviteTime";

    /**
     * 获取师徒邀请缓存信息
     */
    Integer getLastInviteInfoByMasterId(Long masterId, String operDate);

    /**
     * 新增徒弟修改缓存
     */
    void addOneNewApprentice(Long masterId, String operDate);

}
