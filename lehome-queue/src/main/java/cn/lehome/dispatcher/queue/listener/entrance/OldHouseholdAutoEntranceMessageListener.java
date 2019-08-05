package cn.lehome.dispatcher.queue.listener.entrance;

import cn.lehome.base.api.old.pro.bean.entrance.OldEntranceGuardUser;
import cn.lehome.base.api.old.pro.bean.entrance.OldEntranceGuardUserFacilityRelationship;
import cn.lehome.base.api.old.pro.bean.entrance.QOldEntranceGuardUserFacilityRelationship;
import cn.lehome.base.api.old.pro.bean.facility.OldExtendFacility;
import cn.lehome.base.api.old.pro.bean.facility.OldExtendFacilityRelation;
import cn.lehome.base.api.old.pro.bean.facility.QOldExtendFacility;
import cn.lehome.base.api.old.pro.bean.facility.QOldExtendFacilityRelation;
import cn.lehome.base.api.old.pro.bean.household.OldHouseholdsSettingsInfo;
import cn.lehome.base.api.old.pro.service.entrance.OldEntranceGuardUserApiService;
import cn.lehome.base.api.old.pro.service.entrance.OldEntranceGuardUserFacilityRelationApiService;
import cn.lehome.base.api.old.pro.service.facility.OldExtendFacilityApiService;
import cn.lehome.base.api.old.pro.service.household.OldHouseholdsInfoApiService;
import cn.lehome.bean.pro.old.enums.EnabledStatus;
import cn.lehome.bean.pro.old.enums.entrance.EntranceGuardDeviceUserType;
import cn.lehome.bean.pro.old.enums.facility.EntranceGuardType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class OldHouseholdAutoEntranceMessageListener extends AbstractJobListener {

    @Autowired
    private OldHouseholdsInfoApiService oldHouseholdsInfoApiService;

    @Autowired
    private OldExtendFacilityApiService extendFacilityApiService;

    @Autowired
    private OldEntranceGuardUserFacilityRelationApiService oldEntranceGuardUserFacilityRelationApiService;

    @Autowired
    private OldEntranceGuardUserApiService oldEntranceGuardUserApiService;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        OldHouseholdsSettingsInfo oldHouseholdsSettingsInfo = oldHouseholdsInfoApiService.findSettingByHouseholdId(longEventMessage.getData().intValue());
        if (oldHouseholdsSettingsInfo == null) {
            logger.error("住户信息未找到, householdId = " + longEventMessage.getData());
            return;
        }
        OldEntranceGuardUser oldEntranceGuardUser = oldEntranceGuardUserApiService.findByUserTypeAndObjectIdAndAreaId(EntranceGuardDeviceUserType.Households, oldHouseholdsSettingsInfo.getHouseholdsId(), oldHouseholdsSettingsInfo.getAreaId());
        if (oldEntranceGuardUser == null) {
            logger.error("开门用户未找到, householdId = " + longEventMessage.getData());
            return;
        }
        List<OldExtendFacility> doorList = extendFacilityApiService.findFacilityAll(ApiRequest.newInstance().filterEqual(QOldExtendFacility.category, EntranceGuardType.AreaDoor).filterEqual(QOldExtendFacility.areaId, oldHouseholdsSettingsInfo.getAreaId()).filterEqual(QOldExtendFacility.status, EnabledStatus.Enabled));
        if (CollectionUtils.isEmpty(doorList)) {
            doorList = Lists.newArrayList();
        }
        List<OldExtendFacilityRelation> relationList = extendFacilityApiService.findAll(ApiRequest.newInstance().filterEqual(QOldExtendFacilityRelation.unitId, oldHouseholdsSettingsInfo.getUnitId()));
        if (!CollectionUtils.isEmpty(relationList)) {
            Map<Integer, OldExtendFacility> oldExtendFacilityMap = extendFacilityApiService.findExtendAll(relationList.stream().map(OldExtendFacilityRelation::getFacilityId).collect(Collectors.toList()));
            if (!CollectionUtils.isEmpty(oldExtendFacilityMap)) {
                for (OldExtendFacility oldExtendFacility : oldExtendFacilityMap.values()) {
                    doorList.add(oldExtendFacility);
                }
            }
        }

        List<OldEntranceGuardUserFacilityRelationship> oldEntranceGuardUserFacilityRelationships = oldEntranceGuardUserFacilityRelationApiService.findAll(ApiRequest.newInstance().filterEqual(QOldEntranceGuardUserFacilityRelationship.entranceGuardUserId, oldEntranceGuardUser.getId()).filterIn(QOldEntranceGuardUserFacilityRelationship.facilityId, doorList.stream().map(OldExtendFacility::getId).collect(Collectors.toList())));
        List<OldEntranceGuardUserFacilityRelationship> insertList = Lists.newArrayList();
        List<OldEntranceGuardUserFacilityRelationship> updateList = Lists.newArrayList();
        Map<Integer, OldEntranceGuardUserFacilityRelationship> relationshipMap = Maps.newHashMap();
        if (!CollectionUtils.isEmpty(oldEntranceGuardUserFacilityRelationships)) {
            oldEntranceGuardUserFacilityRelationships.forEach(oldEntranceGuardUserFacilityRelationship -> relationshipMap.put(oldEntranceGuardUserFacilityRelationship.getFacilityId(), oldEntranceGuardUserFacilityRelationship));
        }
        for (OldExtendFacility oldExtendFacility : doorList) {
            OldEntranceGuardUserFacilityRelationship oldEntranceGuardUserFacilityRelationship = relationshipMap.get(oldExtendFacility.getId());
            if (oldEntranceGuardUserFacilityRelationship != null) {
                oldEntranceGuardUserFacilityRelationship.setEnabledStatus(EnabledStatus.Enabled);
                updateList.add(oldEntranceGuardUserFacilityRelationship);
            } else {
                oldEntranceGuardUserFacilityRelationship = new OldEntranceGuardUserFacilityRelationship();
                oldEntranceGuardUserFacilityRelationship.setEnabledStatus(EnabledStatus.Enabled);
                oldEntranceGuardUserFacilityRelationship.setEntranceGuardUserId(oldEntranceGuardUser.getId());
                oldEntranceGuardUserFacilityRelationship.setFacilityId(oldExtendFacility.getId());
                insertList.add(oldEntranceGuardUserFacilityRelationship);
            }
        }

        if (!CollectionUtils.isEmpty(insertList)) {
            oldEntranceGuardUserFacilityRelationApiService.batchSave(insertList);
        }
        if (!CollectionUtils.isEmpty(updateList)) {
            oldEntranceGuardUserFacilityRelationApiService.batchSave(updateList);
        }
    }



    @Override
    public String getConsumerId() {
        return "old_household_auto_entrance";
    }
}
