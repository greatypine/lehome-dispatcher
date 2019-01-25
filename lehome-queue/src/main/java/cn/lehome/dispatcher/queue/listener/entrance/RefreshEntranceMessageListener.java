package cn.lehome.dispatcher.queue.listener.entrance;

import cn.lehome.base.api.property.bean.door.DoorOpenInfo;
import cn.lehome.base.api.property.service.door.DoorInfoApiService;
import cn.lehome.base.api.tool.bean.community.CommunityExt;
import cn.lehome.base.api.tool.compoment.jms.EventBusComponent;
import cn.lehome.base.api.tool.service.community.CommunityApiService;
import cn.lehome.base.api.tool.service.community.CommunityCacheApiService;
import cn.lehome.base.api.user.bean.entrance.UserEntrance;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.entrance.UserEntranceApiService;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.tool.entity.enums.community.EditionType;
import cn.lehome.bean.user.entity.enums.user.HouseType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.token.SmartExchangeTokenService;
import cn.lehome.framework.base.api.core.bean.SmartTokenBean;
import cn.lehome.framework.base.api.core.bean.SmartUserTokenBean;
import cn.lehome.framework.base.api.core.compoment.redis.lock.RedisLock;
import cn.lehome.framework.base.api.core.constant.HeaderKeyContants;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.exception.BaseApiException;
import cn.lehome.framework.base.api.core.service.AuthorizationService;
import cn.lehome.framework.base.api.core.util.MD5Util;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class RefreshEntranceMessageListener extends AbstractJobListener {

    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private UserEntranceApiService userEntranceApiService;

    @Autowired
    private DoorInfoApiService doorInfoApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private CommunityApiService communityApiService;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Autowired
    private SmartExchangeTokenService smartExchangeTokenService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static String REFRESH_ENTRANCE = "refresh_entrance_";

    @Value("${property.new.flag}")
    private Boolean isNewFlag;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;

        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(longEventMessage.getData());
        if (userInfoIndex == null) {
            logger.error("用户信息未找到, userId = {}", longEventMessage.getData());
            return;
        }

        List<UserHouseRelationship> userHouseRelationshipList = userHouseRelationshipApiService.findByUserId(longEventMessage.getData());
        if (CollectionUtils.isEmpty(userHouseRelationshipList)) {
            logger.error("没有认证房产, 不用刷新门禁数据");
            return;
        }

        RedisLock redisLock = new RedisLock(stringRedisTemplate, REFRESH_ENTRANCE + userInfoIndex.getId(), 5, TimeUnit.SECONDS);
        try {
            if (redisLock.tryLock()) {
                logger.error("用户 userId = {}, 刷新门禁", userInfoIndex.getId());
                boolean isRefreshPropertyToken = false;
                Map<Long, UserEntrance> maps = Maps.newHashMap();
                List<UserEntrance> userEntranceList = userEntranceApiService.findByUserId(longEventMessage.getData());
                if (!CollectionUtils.isEmpty(userEntranceList)) {
                    logger.error("userEntranceList size ： " + userEntranceList.size());
                    userEntranceList.forEach(userEntrance -> maps.put(userEntrance.getCommunityExtId(), userEntrance));
                }

                Map<String, String> headerMaps = Maps.newHashMap();
                headerMaps.put(HeaderKeyContants.API_CLIENT_ID, authorizationService.getClientId(userInfoIndex.getUserOpenId()));
                headerMaps.put(HeaderKeyContants.API_APP_ID, authorizationService.getAppId(userInfoIndex.getUserOpenId()));
                headerMaps.put(HeaderKeyContants.API_PRO_APP_OAUTH_TOKEN, authorizationService.getApiToken(userInfoIndex.getUserOpenId()));
                headerMaps.put(HeaderKeyContants.EDITION_TYPE_KEY, EditionType.free.toString());
                List<DoorOpenInfo> freeDoorOpenInfoList = doorInfoApiService.findAllDoorOpenInfoOld(userInfoIndex.getId(), headerMaps);
                Map<Long, DoorOpenInfo> freeDoorOpenInfoMap = Maps.newHashMap();
                for (DoorOpenInfo doorOpenInfo : freeDoorOpenInfoList) {
                    CommunityExt communityExt = communityApiService.findByPropertyAreaId(doorOpenInfo.getAreaId());
                    if (communityExt == null) {
                        logger.error("小区信息未找到, areaId = {}", doorOpenInfo.getAreaId());
                        continue;
                    }
                    doorOpenInfo.setAreaId(communityExt.getId());
                    doorOpenInfo.setAreaName(communityExt.getName());
                    freeDoorOpenInfoMap.put(communityExt.getId(), doorOpenInfo);
                }

                List<UserEntrance> addList = Lists.newArrayList();
                List<UserEntrance> updateList = Lists.newArrayList();
                Set<Long> communityExtIds = userHouseRelationshipList.stream().filter(userHouseRelationship -> !userHouseRelationship.getHouseType().equals(HouseType.FORBID)).map(UserHouseRelationship::getCommunityExtId).collect(Collectors.toSet());
                for (Long communityExtId : communityExtIds) {
                    logger.error("获取智社区小区门禁 :" + communityExtId);

                    UserEntrance userEntrance = maps.get(communityExtId);
                    CommunityExt communityExt = communityCacheApiService.getCommunityExt(communityExtId);
                    if (communityExt == null) {
                        logger.error("签约小区未找到, communityExtId = {}", communityExtId);
                        continue;
                    }
                    DoorOpenInfo doorOpenInfo = null;
                    if (communityExt.getEditionType().equals(EditionType.free)) {
                        doorOpenInfo = freeDoorOpenInfoMap.get(communityExt.getId());
                    } else {
                        headerMaps = Maps.newHashMap();
                        if (isNewFlag) {
                            SmartUserTokenBean smartUserTokenBean = authorizationService.getSmartUserToken(userInfoIndex.getUserOpenId(), communityExt.getUniqueCode());
                            if (smartUserTokenBean != null) {
                                headerMaps.put(HeaderKeyContants.Authorization, "Bearer " + smartUserTokenBean.getAccessToken());
                            }
                        } else {
                            SmartTokenBean smartTokenBean = authorizationService.getSmartToken(userInfoIndex.getUserOpenId(), communityExt.getId());
                            if (smartTokenBean != null) {
                                headerMaps.put(HeaderKeyContants.Authorization, smartTokenBean.getSmartToken());
                            }
                        }

                        if (headerMaps.size() != 0) {
                            try {
                                List<DoorOpenInfo> doorOpenInfos = doorInfoApiService.findAllDoorOpenInfoNew(communityExt.getPropertyCommunityId(), headerMaps);
                                if (!CollectionUtils.isEmpty(doorOpenInfos)) {
                                    doorOpenInfo = doorOpenInfos.get(0);
                                    doorOpenInfo.setAreaId(communityExtId);
                                    doorOpenInfo.setAreaName(communityExt.getName());
                                }
                            } catch (BaseApiException e) {
                                if (StringUtils.isNotEmpty(e.getCode()) && e.getCode().equals("PT0003")) {
                                    logger.error("获取智社区门禁列表失败, userId = {}, communityExtId = {}", userInfoIndex.getId(), communityExt.getId());
                                    //eventBusComponent.sendEventMessage(new StringEventMessage(EventConstants.REFRESH_TOKEN_EVENT, userInfoIndex.getUserOpenId()));
                                }
                            }
                        } else {
                            isRefreshPropertyToken = true;
                        }

                    }

                    if (doorOpenInfo != null) {
                        String entranceValue = JSON.toJSONString(doorOpenInfo);
                        String entranceValueMd5 = MD5Util.encoderByMd5(entranceValue);
                        if (userEntrance == null) {
                            logger.error("没有老旧门禁记录, communityExtId = {}", communityExt.getId());
                            userEntrance = new UserEntrance();
                            userEntrance.setEnableStatus(EnableDisableStatus.ENABLE);
                            userEntrance.setCommunityExtId(communityExtId);
                            userEntrance.setEntranceValue(entranceValue);
                            userEntrance.setEntranceValueMd5(entranceValueMd5);
                            userEntrance.setUserId(userInfoIndex.getId());
                            addList.add(userEntrance);
                        } else {
                            logger.error("有老旧门禁记录, communityExtId = {}", communityExt.getId());
                            userEntrance.setEntranceValue(entranceValue);
                            userEntrance.setEnableStatus(EnableDisableStatus.ENABLE);
                            userEntrance.setEntranceValueMd5(entranceValueMd5);
                            updateList.add(userEntrance);
                        }
                    } else {
                        logger.error("获取门禁列表为空, 进行任何处理");
                    }

                }

                for (Long communityExtId : maps.keySet()) {
                    if (!communityExtIds.contains(communityExtId)) {
                        UserEntrance userEntrance = maps.get(communityExtId);
                        userEntrance.setEnableStatus(EnableDisableStatus.DISABLE);
                        updateList.add(userEntrance);
                    }
                }

                if (!CollectionUtils.isEmpty(addList)) {
                    addList.forEach(userEntranceApiService::save);
                }

                if (!CollectionUtils.isEmpty(updateList)) {
                    updateList.forEach(userEntranceApiService::update);
                }

                if (isRefreshPropertyToken) {
                    logger.error("重新获取智社区token");
                    smartExchangeTokenService.exchangeAll(userInfoIndex.getUserOpenId());
                }
            } else {
                logger.error("获取锁失败");
            }
        } catch (Exception e) {
            logger.error("刷新门禁列表失败 : ", e);
        } finally {
            redisLock.unlock();
        }


    }



    @Override
    public String getConsumerId() {
        return "refresh_entrance";
    }
}
