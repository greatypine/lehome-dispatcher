package cn.lehome.dispatcher.queue.service.impl.house;

import cn.lehome.base.api.advertising.constant.JoinActivityTypeConstants;
import cn.lehome.base.api.common.bean.community.Community;
import cn.lehome.base.api.common.bean.community.CommunityExt;
import cn.lehome.base.api.common.bean.device.ClientDeviceIndex;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.base.api.common.event.JoinActivityEventBean;
import cn.lehome.base.api.common.service.community.CommunityApiService;
import cn.lehome.base.api.common.service.community.CommunityCacheApiService;
import cn.lehome.base.api.common.service.device.ClientDeviceIndexApiService;
import cn.lehome.base.api.property.bean.households.AuthHouseholdsInfo;
import cn.lehome.base.api.property.service.households.HouseholdsInfoApiService;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.common.enums.community.EditionType;
import cn.lehome.bean.user.entity.enums.user.HouseType;
import cn.lehome.dispatcher.queue.service.house.HouseholdService;
import cn.lehome.dispatcher.queue.service.impl.AbstractBaseServiceImpl;
import cn.lehome.framework.actionlog.core.ActionLogRequest;
import cn.lehome.framework.actionlog.core.bean.ActionLog;
import cn.lehome.framework.actionlog.core.bean.AppActionLog;
import cn.lehome.framework.base.api.core.constant.HeaderKeyContants;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.event.StringEventMessage;
import cn.lehome.framework.base.api.core.service.AuthorizationService;
import cn.lehome.framework.base.api.core.util.StringUtil;
import cn.lehome.framework.bean.core.enums.ClientOSType;
import cn.lehome.framework.bean.core.enums.ClientType;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import cn.lehome.framework.constant.HttpLogConstants;
import cn.lehome.framework.constant.UserActionKeyConstants;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by zuoguodong on 2018/6/4
 */
@Service("householdService")
public class HouseholdServiceImpl extends AbstractBaseServiceImpl implements HouseholdService {

    @Autowired
    private HouseholdsInfoApiService householdsInfoApiService;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private CommunityApiService communityApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ClientDeviceIndexApiService clientDeviceIndexApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private ActionLogRequest actionLogRequest;

    private static final Integer IOS_MIN_VERSION_CODE = 320;
    private static final Integer ANDROID_MIN_VERSION_CODE = 3200;

    @Override
    public void syncHouseholdInfo(UserInfoIndex userInfoIndex) {
        Map<String, String> headerMaps = Maps.newHashMap();
        headerMaps.put(HeaderKeyContants.API_CLIENT_ID, authorizationService.getClientId(userInfoIndex.getUserOpenId()));
        headerMaps.put(HeaderKeyContants.API_APP_ID, authorizationService.getAppId(userInfoIndex.getUserOpenId()));
        headerMaps.put(HeaderKeyContants.API_PRO_APP_OAUTH_TOKEN, authorizationService.getApiToken(userInfoIndex.getUserOpenId()));

        List<AuthHouseholdsInfo> authList = Lists.newArrayList();
        logger.error("同步房产开始, userId = " + userInfoIndex.getId());
        boolean oldPropertyIsOk = true;
        try {
            headerMaps.put(HeaderKeyContants.EDITION_TYPE_KEY, EditionType.free.toString());
            List<AuthHouseholdsInfo> freeAuthHouseholdsInfoList = householdsInfoApiService.relateByPhone(userInfoIndex.getPhone(), userInfoIndex.getId(), headerMaps);
            if (!CollectionUtils.isEmpty(freeAuthHouseholdsInfoList)) {
                authList.addAll(freeAuthHouseholdsInfoList);
            }
        } catch (Exception e) {
            oldPropertyIsOk = false;
            logger.error("获取free认证房产信息错误", e);
        }

        boolean newPropertyIsOk = true;
        try {
            headerMaps.put(HeaderKeyContants.EDITION_TYPE_KEY, EditionType.pro.toString());
            List<AuthHouseholdsInfo> proAuthHouseholdsInfoList = householdsInfoApiService.relateByPhone(userInfoIndex.getPhone(), userInfoIndex.getId(), headerMaps);
            if (!CollectionUtils.isEmpty(proAuthHouseholdsInfoList)) {
                authList.addAll(proAuthHouseholdsInfoList);
            }
        } catch (Exception e) {
            newPropertyIsOk = false;
            logger.error("获取pro认证房产信息错误", e);
        }

        if (!CollectionUtils.isEmpty(authList)) {
            List<UserHouseRelationship> updateList = Lists.newArrayList();
            List<UserHouseRelationship> insertList = Lists.newArrayList();
            List<UserHouseRelationship> userHouseRelationshipList = userHouseRelationshipApiService.findByRemark(userInfoIndex.getPhone());
            Map<String, UserHouseRelationship> maps = Maps.newHashMap();
            Map<String, AuthHouseholdsInfo> authMaps = Maps.newHashMap();
            if (!CollectionUtils.isEmpty(userHouseRelationshipList)) {
                for (UserHouseRelationship relationship : userHouseRelationshipList) {
                    maps.put(String.format("%s-%s", relationship.getCommunityExtId(), relationship.getHouseId()), relationship);
                }
            }

            for (AuthHouseholdsInfo authHouseholdsInfo : authList) {
                authMaps.put(String.format("%s-%s", authHouseholdsInfo.getAreaId(), authHouseholdsInfo.getHouseId()), authHouseholdsInfo);
            }

            for (AuthHouseholdsInfo authHouseholdsInfo : authList) {
                CommunityExt communityExt = communityApiService.findByPropertyAreaId(authHouseholdsInfo.getAreaId());
                if (communityExt == null) {
                    logger.error("未找到物管绑定小区, areaId = {}", authHouseholdsInfo.getAreaId());
                    continue;
                }
                String fullAddressPrefix = "";
                List<Long> communityIds = communityCacheApiService.getCommunityExtBind(communityExt.getId());
                if (!CollectionUtils.isEmpty(communityIds)) {
                    Community community = communityCacheApiService.getCommunity(communityIds.get(0));
                    fullAddressPrefix = community.getFullAddress();
                }
                String key = String.format("%s-%s", communityExt.getId(), authHouseholdsInfo.getHouseId());
                UserHouseRelationship userHouseRelationship = maps.get(key);
                if (userHouseRelationship != null) {
                    userHouseRelationship.setHouseType(convertType(authHouseholdsInfo.getHouseHoldTypeId()));
                    userHouseRelationship.setEnableStatus(EnableDisableStatus.ENABLE);
                    updateList.add(userHouseRelationship);
                } else {
                    userHouseRelationship = new UserHouseRelationship();
                    userHouseRelationship.setEnableStatus(EnableDisableStatus.ENABLE);
                    userHouseRelationship.setHouseType(convertType(authHouseholdsInfo.getHouseHoldTypeId()));
                    userHouseRelationship.setRemark(userInfoIndex.getPhone());
                    userHouseRelationship.setUserId(userInfoIndex.getId());
                    userHouseRelationship.setCommunityExtId(communityExt.getId());
                    userHouseRelationship.setHouseId(authHouseholdsInfo.getHouseId());
                    userHouseRelationship.setHouseAddress(authHouseholdsInfo.getAddress());
                    userHouseRelationship.setFullHouseAddress(fullAddressPrefix + authHouseholdsInfo.getAddress());
                    insertList.add(userHouseRelationship);
                }
            }

            if (!CollectionUtils.isEmpty(userHouseRelationshipList)) {
                for (UserHouseRelationship userHouseRelationship : userHouseRelationshipList) {
                    if (userHouseRelationship.getHouseType().equals(HouseType.FORBID)) {
                        continue;
                    }
                    CommunityExt communityExt = communityCacheApiService.getCommunityExt(userHouseRelationship.getCommunityExtId());
                    if (communityExt == null) {
                        logger.error("未找到物管绑定小区, extId = {}", userHouseRelationship.getCommunityExtId());
                        continue;
                    }
                    if (!oldPropertyIsOk && communityExt.getEditionType().equals(EditionType.free)) {
                        continue;
                    }
                    if (!newPropertyIsOk && communityExt.getEditionType().equals(EditionType.pro)) {
                        continue;
                    }
                    String key = String.format("%s-%s", communityExt.getPropertyCommunityId(), userHouseRelationship.getHouseId());
                    if (!authMaps.containsKey(key)) {
                        logger.error("禁用房产, key = {}", key);
                        userHouseRelationship.setHouseType(HouseType.FORBID);
                        updateList.add(userHouseRelationship);
                    }
                }
            }

            boolean isHasHouse = false;
            Long userHouseRelationshipId = 0L;

            String clientId = StringUtil.defaultIfBlank(StringUtil.defaultIfBlank(userInfoIndex.getClientId(), userInfoIndex.getLastClientId()), "0");
            ActionLog.Builder builder = ActionLog.newBuilder();
            if (!CollectionUtils.isEmpty(insertList)) {
                insertList.forEach(p -> {
                    UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.saveUserHouse(p);

                    AppActionLog.Builder appBuilder = AppActionLog.newBuilder(UserActionKeyConstants.USER_HOUSE_NEW, userInfoIndex.getUserOpenId(), clientId);
                    setActionLog(appBuilder, userHouseRelationship.getCommunityExtId(), clientId);
                    builder.addActionLogBean(appBuilder.build());
                });
                List<UserHouseRelationship> userHouseRelationships = userHouseRelationshipApiService.findByUserId(userInfoIndex.getId());
                userHouseRelationshipId = userHouseRelationships.get(0).getId();
                isHasHouse = true;
            }
            if (!CollectionUtils.isEmpty(updateList)) {
                updateList.forEach(p -> {
                    UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.update(p);

                    String actionKey = UserActionKeyConstants.USER_HOUSE_UPDATE;
                    if (HouseType.FORBID.equals(userHouseRelationship.getHouseType())) {
                        actionKey = UserActionKeyConstants.USER_HOUSE_REMOVE;
                    }
                    AppActionLog.Builder appBuilder = AppActionLog.newBuilder(actionKey, userInfoIndex.getUserOpenId(), clientId);
                    if (UserActionKeyConstants.USER_HOUSE_REMOVE.equals(actionKey)) {
                        setActionLog(appBuilder, userHouseRelationship.getCommunityExtId(), clientId);
                    }
                    builder.addActionLogBean(appBuilder.build());
                });

                userHouseRelationshipId = updateList.get(0).getId();
                isHasHouse = true;
            }
            // region 2018年09月18日 用户房产相关埋点
            builder.send(actionLogRequest);
            // endregion

            if (isHasHouse) {
                // 发送参加活动消息
                logger.error("有房产变动, 新增{}, 更新{}", insertList.size(), updateList.size());
                JoinActivityEventBean joinActivityEventBean = new JoinActivityEventBean();
                joinActivityEventBean.setJoinActivityType(JoinActivityTypeConstants.USER_AUTH_HOUSE);
                List<Object> attributes = Lists.newArrayList();
                attributes.add(userHouseRelationshipId);
                attributes.add(userInfoIndex.getId());
                joinActivityEventBean.setAttributes(attributes);
                if (isSendMegToNewQueue(userInfoIndex.getClientId())) {
                    eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.JOIN_REWARD_ACTIVITY_EVENT, joinActivityEventBean));
                } else {
                    eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.JOIN_ACTIVITY_EVENT, joinActivityEventBean));
                }
                //发送换取token接口
                StringEventMessage exchangeEventMessage = new StringEventMessage(EventConstants.EXCAHNGE_SMART_TOKEN_EVENT, userInfoIndex.getUserOpenId());
                eventBusComponent.sendEventMessage(exchangeEventMessage);
            }
        }
        logger.error("同步房产结束, userId = " + userInfoIndex.getId());
    }

    /**
     * Send action log.
     *
     * @param builder        the builder
     * @param communityExtId the community ext id
     * @param clientId       the client id
     *
     * @author zhuzz
     * @time 2018 /10/19 15:18:05
     */
    private void setActionLog(AppActionLog.Builder builder, Long communityExtId, String clientId) {
        builder.addMap(HttpLogConstants.COMMUNITY_EXT_ID, communityExtId);
        List<Long> communityIds = communityCacheApiService.getCommunityExtBind(communityExtId);
        if (com.alibaba.dubbo.common.utils.CollectionUtils.isNotEmpty(communityIds)) {
            Community community = communityCacheApiService.getCommunity(communityIds.get(0));
            if (Objects.nonNull(community)) {
                builder.addMap(HttpLogConstants.COMMUNITY_ID, community.getId());
            }
        }
        ClientDeviceIndex clientDeviceIndex = clientDeviceIndexApiService.get(clientId, ClientType.SQBJ);
        if (Objects.nonNull(clientDeviceIndex)) {
            builder.addMap(HttpLogConstants.VERSION, clientDeviceIndex.getAppVersion());
        }
    }

    private HouseType convertType(Long householdsTypeId) {
        if (householdsTypeId == 1) {
            return HouseType.MAIN;
        } else if (householdsTypeId == 6) {
            return HouseType.RENTER;
        } else {
            return HouseType.HOME;
        }
    }

    private boolean isSendMegToNewQueue(String clientId) {
        ClientDeviceIndex clientDeviceIndex = clientDeviceIndexApiService.get(clientId, ClientType.SQBJ);
        if (ClientOSType.IOS.equals(clientDeviceIndex.getClientOSType())) {
            return IOS_MIN_VERSION_CODE <= clientDeviceIndex.getAppVersionCode();
        } else if (ClientOSType.ANDROID.equals(clientDeviceIndex.getClientOSType())) {
            return ANDROID_MIN_VERSION_CODE <= clientDeviceIndex.getAppVersionCode();
        }
        return false;
    }

}
