package cn.lehome.dispatcher.queue.listener.entrance;

import cn.lehome.base.api.acs.bean.user.User;
import cn.lehome.base.pro.api.bean.households.HouseholdIndex;
import cn.lehome.base.pro.api.bean.households.QHouseholdIndex;
import cn.lehome.base.pro.api.service.households.HouseholdIndexApiService;
import cn.lehome.bean.pro.enums.RecordDeleteStatus;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.entrance.AutoEntranceService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class HouseholdAutoEntranceMessageListener extends AbstractJobListener {

    @Autowired
    private HouseholdIndexApiService householdIndexApiService;

    @Autowired
    private AutoEntranceService autoEntranceService;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        HouseholdIndex householdIndex = householdIndexApiService.get(longEventMessage.getData());
        if (householdIndex == null) {
            logger.error("住户信息未找到, id = " + longEventMessage.getData());
            return;
        }

        User user = autoEntranceService.getUserByHousehold(householdIndex);

        List<HouseholdIndex> householdIndices = householdIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseholdIndex.openId, householdIndex.getOpenId()).filterEqual(QHouseholdIndex.areaId, householdIndex.getAreaId()).filterEqual(QHouseholdIndex.deleteStatus, RecordDeleteStatus.Normal));

        if (!CollectionUtils.isEmpty(householdIndices)) {
            autoEntranceService.modifyUserRegion(user, householdIndices);
        } else {
            autoEntranceService.deleteAllUserRegion(user, householdIndex.getAreaId());
        }
    }



    @Override
    public String getConsumerId() {
        return "household_auto_entrance";
    }
}
