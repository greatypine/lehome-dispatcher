package cn.lehome.dispatcher.queue.listener.entrance;

import cn.lehome.base.api.acs.bean.user.User;
import cn.lehome.base.api.common.business.oauth2.bean.user.*;
import cn.lehome.base.api.common.business.oauth2.service.user.UserAccountIndexApiService;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.entrance.AutoEntranceService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class EmployeeAutoEntranceMessageListener extends AbstractJobListener {


    @Autowired
    private AutoEntranceService autoEntranceService;

    @Autowired
    private UserAccountIndexApiService businessUserAccountIndexApiService;


    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }

        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        logger.error("员工开始同步创建开门用户 : " + longEventMessage.getData());
        List<Oauth2AccountIndex> oauth2AccountIndexList = businessUserAccountIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QOauth2AccountIndex.accountId, longEventMessage.getData()).filterEqual(QOauth2AccountIndex.clientId, "sqbj-smart"));
        Oauth2AccountIndex oauth2AccountIndex = null;
        if (!CollectionUtils.isEmpty(oauth2AccountIndexList)) {
            oauth2AccountIndex = oauth2AccountIndexList.get(0);
        }
        if (oauth2AccountIndex == null) {
            logger.error("用户信息未找到用户Oauth信息, accountId = " + longEventMessage.getData());
            return;
        }
        User user = autoEntranceService.getUserByAccount(oauth2AccountIndex);

        List<UserAccountAreaIndex> userAccountAreaIndexList = businessUserAccountIndexApiService.findAreaAll(ApiRequest.newInstance().filterEqual(QUserAccountAreaIndex.accountId, longEventMessage.getData()).filterEqual(QUserAccountAreaIndex.disableStatus, EnableDisableStatus.ENABLE));
        if (CollectionUtils.isEmpty(userAccountAreaIndexList)) {
            logger.error("用户没有小区授权, 无需门禁授权");
            return;
        }

        for (UserAccountAreaIndex userAccountAreaIndex : userAccountAreaIndexList) {
            autoEntranceService.modifyUserRegionByArea(user, userAccountAreaIndex.getAreaId());
        }
        logger.error("员工结束同步创建开门用户 : " + longEventMessage.getData());
    }



    @Override
    public String getConsumerId() {
        return "employee_auto_entrance";
    }
}
