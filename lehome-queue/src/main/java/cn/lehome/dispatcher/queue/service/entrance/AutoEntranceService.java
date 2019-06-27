package cn.lehome.dispatcher.queue.service.entrance;

import cn.lehome.base.api.acs.bean.user.User;
import cn.lehome.base.api.common.business.oauth2.bean.user.Oauth2AccountIndex;
import cn.lehome.base.pro.api.bean.households.HouseholdIndex;
import cn.lehome.base.pro.api.bean.regions.ControlRegions;

import java.util.List;

/**
 * Created by wuzhao on 2019/6/21.
 */
public interface AutoEntranceService {

    User getUserByHousehold(HouseholdIndex householdIndex);

    User getUserByAccount(Oauth2AccountIndex oauth2AccountIndex);

    void modifyUserRegion(User user, List<HouseholdIndex> householdIndices);

    void deleteAllUserRegion(User user, Long areaId);

    void modifyUserRegionByArea(User user, Long areaId);

    List<User> loadAllUser(ControlRegions controlRegions);
}
