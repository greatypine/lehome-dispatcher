package cn.lehome.dispatcher.queue.service.house;

import cn.lehome.base.api.user.bean.user.UserInfoIndex;

/**
 * Created by zuoguodong on 2018/6/4
 */
public interface HouseholdService {

    /**
     * 通过OPENID同步房产信息
     * @param userInfoIndex
     */
    void syncHouseholdInfo(UserInfoIndex userInfoIndex);

}
