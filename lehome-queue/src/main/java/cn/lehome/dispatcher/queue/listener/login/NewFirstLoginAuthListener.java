package cn.lehome.dispatcher.queue.listener.login;

import cn.lehome.base.api.common.bean.community.CommunityExt;
import cn.lehome.base.api.common.custom.oauth2.bean.user.UserAccountIndex;
import cn.lehome.base.api.common.custom.oauth2.service.user.UserAccountIndexApiService;
import cn.lehome.base.api.common.service.community.CommunityApiService;
import cn.lehome.base.api.old.pro.bean.house.OldHouseInfo;
import cn.lehome.base.api.old.pro.bean.household.OldHouseholdsInfo;
import cn.lehome.base.api.old.pro.bean.household.OldHouseholdsSettingsInfo;
import cn.lehome.base.api.old.pro.service.house.OldHouseInfoApiService;
import cn.lehome.base.api.old.pro.service.household.OldHouseholdsInfoApiService;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.base.pro.api.bean.house.HouseInfoIndex;
import cn.lehome.base.pro.api.bean.households.HouseholdIndex;
import cn.lehome.base.pro.api.bean.households.QHouseholdIndex;
import cn.lehome.base.pro.api.service.house.HouseInfoIndexApiService;
import cn.lehome.base.pro.api.service.households.HouseholdIndexApiService;
import cn.lehome.bean.user.entity.enums.user.HouseType;
import cn.lehome.bean.workorder.enums.RecordDeleteStatus;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class NewFirstLoginAuthListener extends AbstractJobListener {

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService;

    @Autowired
    private UserAccountIndexApiService userAccountIndexApiService;

    @Autowired
    private HouseholdIndexApiService householdIndexApiService;

    @Autowired
    private OldHouseInfoApiService oldHouseInfoApiService;

    @Autowired
    private HouseInfoIndexApiService smartHouseInfoIndexApiService;

    @Autowired
    private OldHouseholdsInfoApiService oldHouseholdsInfoApiService;

    @Autowired
    private CommunityApiService communityApiService;

    @Value("${current.platform}")
    private String currentPlatForm;

    @Value("${dev.ids}")
    private String devIds;

    @Value("%{qa.ids}")
    private String qaIds;



    @Override
    public void execute(IEventMessage eventMessage){
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        UserAccountIndex userAccountIndex = userAccountIndexApiService.getUserAccount(longEventMessage.getData().toString());
        if (userAccountIndex == null) {
            logger.error("用户信息未找到");
            return;
        }
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByPhone(userAccountIndex.getPhone());
        if (userInfoIndex == null) {
            logger.error("用户信息未找到");
            return;
        }
        List<UserHouseRelationship> userHouseRelationships = Lists.newArrayList();

        logger.error("智社区同步认证信息");
        try {
            List<HouseholdIndex> householdIndices = householdIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseholdIndex.userId, userAccountIndex.getId()).filterEqual(QHouseholdIndex.deleteStatus, RecordDeleteStatus.Normal.toString()));
            if (!CollectionUtils.isEmpty(householdIndices)) {
                logger.error("同步智社区认证信息条数: " + householdIndices.size());
                for (HouseholdIndex householdIndex : householdIndices) {
                    UserHouseRelationship userHouseRelationship = smartToRelationship(householdIndex, userInfoIndex.getId());
                    if (userHouseRelationship != null) {
                        userHouseRelationships.add(userHouseRelationship);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("智社区同步认证失败 : ", e);
        }

        logger.error("半径管家同步认证信息");
        try {
            List<OldHouseholdsInfo> oldHouseholdsInfos = oldHouseholdsInfoApiService.findByPhone(userAccountIndex.getPhone());
            if (!CollectionUtils.isEmpty(oldHouseholdsInfos)) {
                List<OldHouseholdsInfo> filterList = Lists.newArrayList();
                for (OldHouseholdsInfo oldHouseholdsInfo : oldHouseholdsInfos) {
                    OldHouseholdsSettingsInfo oldHouseholdsSettingsInfo = oldHouseholdsInfoApiService.findSettingByHouseholdId(oldHouseholdsInfo.getId());
                    if (oldHouseholdsSettingsInfo == null) {
                        continue;
                    }
                    if (currentPlatForm.equals("dev")) {
                        if (!getDevIds().contains(oldHouseholdsSettingsInfo.getAreaId())) {
                            continue;
                        }
                    }
                    if (currentPlatForm.equals("qa")) {
                        if (!getQaIds().contains(oldHouseholdsSettingsInfo.getAreaId())) {
                            continue;
                        }
                    }
                    if (currentPlatForm.equals("release")) {
                        if (getAllIds().contains(oldHouseholdsSettingsInfo.getAreaId())) {
                            continue;
                        }
                    }
                    oldHouseholdsInfo.setOldHouseholdsSettingsInfo(oldHouseholdsSettingsInfo);
                    filterList.add(oldHouseholdsInfo);
                }
                if (!CollectionUtils.isEmpty(filterList)) {
                    logger.error("同步半径管家认证信息条数: " + filterList.size());
                    for (OldHouseholdsInfo oldHouseholdsInfo : filterList) {
                        UserHouseRelationship userHouseRelationship = bjgjToRelationship(oldHouseholdsInfo, userInfoIndex.getId());
                        if (userHouseRelationship != null) {
                            userHouseRelationships.add(userHouseRelationship);
                        }
                        oldHouseholdsInfoApiService.bindUserId(oldHouseholdsInfo.getId(), userInfoIndex.getId().intValue());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("半径管家管家同步认证失败 : ", e);
        }


        logger.error("同步的认证信息条数: " + userHouseRelationships.size());
        for (UserHouseRelationship userHouseRelationship : userHouseRelationships) {
            userHouseRelationshipApiService.saveUserHouse(userHouseRelationship);
        }
    }

    private UserHouseRelationship smartToRelationship(HouseholdIndex householdIndex, Long oldUserId) {
        CommunityExt communityExt = communityApiService.findByPropertyAreaId(householdIndex.getAreaId());
        HouseInfoIndex houseInfoIndex = smartHouseInfoIndexApiService.get(householdIndex.getHouseId());
        if (communityExt == null) {
            return null;
        }
        if (householdIndex == null) {
            return null;
        }
        UserHouseRelationship userHouseRelationship = new UserHouseRelationship();
        userHouseRelationship.setUserId(oldUserId);
        userHouseRelationship.setHouseId(householdIndex.getHouseId());
        userHouseRelationship.setCommunityExtId(communityExt.getId());
        userHouseRelationship.setFamilyMemberName(householdIndex.getName());
        userHouseRelationship.setRemark(householdIndex.getTelephone());
        userHouseRelationship.setEnableStatus(EnableDisableStatus.ENABLE);
        userHouseRelationship.setHouseType(convertType(householdIndex.getHouseholdsTypeId()));
        userHouseRelationship.setHouseAddress(houseInfoIndex.getRoomAddress());
        userHouseRelationship.setFullHouseAddress(String.format("%s%s", communityExt.getName(), houseInfoIndex.getRoomAddress()));
        userHouseRelationship.setOperatorId(0L);
        return userHouseRelationship;
    }

    private UserHouseRelationship bjgjToRelationship(OldHouseholdsInfo oldHouseholdsInfo, Long oldUserId) {
        CommunityExt communityExt = communityApiService.findByPropertyAreaId(oldHouseholdsInfo.getOldHouseholdsSettingsInfo().getAreaId().longValue());
        OldHouseInfo oldHouseInfo = oldHouseInfoApiService.get(oldHouseholdsInfo.getOldHouseholdsSettingsInfo().getHouseId());
        if (communityExt == null) {
            return null;
        }
        if (oldHouseInfo == null) {
            return null;
        }
        String roomAddress = oldHouseInfo.getManagerAreaName();
        if (StringUtils.isNotEmpty(oldHouseInfo.getFloorNo())) {
            roomAddress = roomAddress + "-" + oldHouseInfo.getFloorNo();
        }
        if (StringUtils.isNotEmpty(oldHouseInfo.getUnitNo())) {
            roomAddress = roomAddress + "-" + oldHouseInfo.getUnitNo();
        }
        roomAddress = roomAddress + "-" + oldHouseInfo.getRoomId();
        UserHouseRelationship userHouseRelationship = new UserHouseRelationship();
        userHouseRelationship.setUserId(oldUserId);
        userHouseRelationship.setHouseId(oldHouseInfo.getId().longValue());
        userHouseRelationship.setCommunityExtId(communityExt.getId());
        userHouseRelationship.setFamilyMemberName(oldHouseholdsInfo.getName());
        userHouseRelationship.setRemark(oldHouseholdsInfo.getTelephone());
        userHouseRelationship.setEnableStatus(EnableDisableStatus.ENABLE);
        userHouseRelationship.setHouseType(convertType(oldHouseholdsInfo.getOldHouseholdsSettingsInfo().getHouseholdsTypeId()));
        userHouseRelationship.setHouseAddress(roomAddress);
        userHouseRelationship.setFullHouseAddress(String.format("%s%s", communityExt.getName(), roomAddress));
        userHouseRelationship.setOperatorId(0L);
        return userHouseRelationship;
    }

    private HouseType convertType(Integer type) {
        if (type == 1) {
            return HouseType.MAIN;
        } else if (type == 5) {
            return HouseType.HOME;
        } else if (type == 6) {
            return HouseType.RENTER;
        } else {
            return HouseType.OTHER;
        }
    }

    @Override
    public String getConsumerId() {
        return "new_first_login_auth";
    }

    private List<Integer> getDevIds() {
        String[] args = devIds.split(",");
        List<Integer> ids = Lists.newArrayList();
        for (String arg : args) {
            if (StringUtils.isNotEmpty(arg)) {
                ids.add(Integer.valueOf(arg));
            }
        }
        return ids;
    }

    private List<Integer> getQaIds() {
        String[] args = qaIds.split(",");
        List<Integer> ids = Lists.newArrayList();
        for (String arg : args) {
            if (StringUtils.isNotEmpty(arg)) {
                ids.add(Integer.valueOf(arg));
            }
        }
        return ids;
    }

    private List<Integer> getAllIds() {
        List<Integer> ids = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(getDevIds())) {
            ids.addAll(getAllIds());
        }
        if (!CollectionUtils.isEmpty(getQaIds())) {
            ids.addAll(getQaIds());
        }
        return ids;
    }
}
