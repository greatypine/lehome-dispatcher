package cn.lehome.dispatcher.quartz.service.invoke.card;

import cn.lehome.base.api.business.activity.bean.advert.Advert;
import cn.lehome.base.api.business.activity.bean.advert.AdvertDeliverRange;
import cn.lehome.base.api.business.activity.bean.advert.AdvertDeliverTimeline;
import cn.lehome.base.api.business.activity.bean.advert.QAdvert;
import cn.lehome.base.api.business.activity.bean.card.AdvertCollectCardCommonCacheBean;
import cn.lehome.base.api.business.activity.bean.card.AdvertCollectCardRecord;
import cn.lehome.base.api.business.activity.bean.card.QAdvertCollectCardRecord;
import cn.lehome.base.api.business.activity.service.advert.ActivityAdvertRedisCache;
import cn.lehome.base.api.business.activity.service.advert.AdvertApiService;
import cn.lehome.base.api.business.activity.service.advert.AdvertDeliverRangeApiService;
import cn.lehome.base.api.business.activity.service.advert.AdvertDeliverTimelineApiService;
import cn.lehome.base.api.business.activity.service.card.AdvertAdditionalApiService;
import cn.lehome.base.api.business.activity.service.card.AdvertCollectCardRecordApiService;
import cn.lehome.base.api.common.bean.device.ClientDevice;
import cn.lehome.base.api.common.bean.device.ClientDeviceIndex;
import cn.lehome.base.api.common.bean.device.PushDeviceInfo;
import cn.lehome.base.api.common.bean.device.QClientDeviceIndex;
import cn.lehome.base.api.common.component.message.push.PushComponent;
import cn.lehome.base.api.common.constant.MessageKeyConstants;
import cn.lehome.base.api.common.service.device.ClientDeviceIndexApiService;
import cn.lehome.base.api.common.service.device.DeviceApiService;
import cn.lehome.base.api.user.bean.user.QUserInfoIndex;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.business.activity.enums.advert.AdvertStatus;
import cn.lehome.bean.business.activity.enums.advert.AdvertTimeLineType;
import cn.lehome.bean.business.activity.enums.advert.AdvertType;
import cn.lehome.bean.business.activity.enums.advert.DeliverRangeType;
import cn.lehome.bean.business.activity.enums.card.AdvertAdditionalType;
import cn.lehome.bean.business.activity.enums.task.AssetType;
import cn.lehome.bean.user.entity.enums.user.UserStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.StringUtil;
import cn.lehome.framework.bean.core.enums.*;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author yanwenkai
 * @date 2018/10/22
 */
@Service("collectCardAllPushMessageSchedulerService")
public class CollectCardAllPushMessageSchedulerServiceImpl extends AbstractInvokeServiceImpl {

    private static final Integer PAGE_SIZE = 100;

    private static final Integer JIGUANG_PUSH_MAX = 500;

    @Autowired
    private AdvertApiService advertApiService;

    @Autowired
    private AdvertDeliverTimelineApiService advertDeliverTimelineApiService;

    @Autowired
    private AdvertDeliverRangeApiService advertDeliverRangeApiService;

    @Autowired
    private ActivityAdvertRedisCache.ActivityCollectCardRedisCache activityCollectCardRedisCache;

    @Autowired
    private AdvertCollectCardRecordApiService advertCollectCardRecordApiService;

    @Autowired
    private AdvertAdditionalApiService advertAdditionalApiService;

    @Autowired
    private DeviceApiService deviceApiService;

    @Autowired
    private ClientDeviceIndexApiService clientDeviceIndexApiService;

    @Autowired
    private PushComponent pushComponent;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Value("${collect.card.remind.push.enable}")
    private boolean enable;

    @Override
    public void doInvoke(Map<String, String> params) {
        if (enable) {
            ApiRequest apiRequest = ApiRequest.newInstance();
            apiRequest.filterEqual(QAdvert.status,AdvertStatus.PUBLISHED);
            apiRequest.filterEqual(QAdvert.type, AdvertType.CARD_COLLECTING);
            apiRequest.filterEqual(QAdvert.isSendPush,YesNoStatus.NO);
            List<Advert> publishedCollectCardAdvertList = advertApiService.findAll(apiRequest);
            if (publishedCollectCardAdvertList != null && publishedCollectCardAdvertList.size() != 0) {
                logger.info("集卡中的集卡活动, 符合条件的广告共有{}个", publishedCollectCardAdvertList.size());
                for (Advert advert : publishedCollectCardAdvertList) {
                    List<AdvertDeliverTimeline> timelineList = advertDeliverTimelineApiService.findAllByAdvertId(advert.getId());
                    if (timelineList != null && timelineList.size() != 0) {
                        AdvertDeliverTimeline advertDeliverTimeline = null;
                        for (AdvertDeliverTimeline timeline : timelineList) {
                            if (timeline.getType().equals(AdvertTimeLineType.CARD)) {
                                advertDeliverTimeline = timeline;
                                break;
                            }
                        }
                        if (advertDeliverTimeline != null && DateUtils.isSameDay(advertDeliverTimeline.getStartDate(), new Date(System.currentTimeMillis()))) {
                            logger.info("集卡开始首天, 发送推送, advertId = {}", advert.getId());
                            List<AdvertDeliverRange> list = advertDeliverRangeApiService.findByAdvertId(advert.getId());
                            this.sendMessage(list, advert.getId(),advert.getAssetType());
                        }
                    }
                }
            }

            apiRequest = ApiRequest.newInstance();
            apiRequest.filterEqual(QAdvert.status, AdvertStatus.OPENING);
            apiRequest.filterEqual(QAdvert.type,AdvertType.CARD_COLLECTING);
            List<Advert> collectCardAdvertList = advertApiService.findAll(apiRequest);
            if (collectCardAdvertList != null && collectCardAdvertList.size() != 0) {
                logger.info("开奖中的集卡活动, 符合条件的广告共有{}个", collectCardAdvertList.size());

                for (Advert advert : collectCardAdvertList) {
                    List<AdvertDeliverTimeline> timelineEntityList = advertDeliverTimelineApiService.findAllByAdvertId(advert.getId());
                    if (timelineEntityList != null && timelineEntityList.size() != 0) {
                        AdvertDeliverTimeline advertDeliverTimeline = null;
                        for (AdvertDeliverTimeline timeline : timelineEntityList) {
                            if (timeline.getType().equals(AdvertTimeLineType.RED_PACKET)) {
                                advertDeliverTimeline = timeline;
                                break;
                            }
                        }
                        if (advertDeliverTimeline != null && DateUtils.isSameDay(advertDeliverTimeline.getStartDate(), new Date(System.currentTimeMillis()))) {
                            logger.info("开奖开始首天, 发送推送, advertId = {}", advert.getId());
                            AdvertCollectCardCommonCacheBean commonCacheBean = activityCollectCardRedisCache.getCollectCardCommonData(advert.getId());
                            Integer isAllStatus = commonCacheBean.getIsAllStatus();
                            int pageIndex = 0;
                            apiRequest = ApiRequest.newInstance();
                            apiRequest.filterEqual(QAdvertCollectCardRecord.advertId,advert.getId());
                            ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
                            apiRequestPage.paging(pageIndex,PAGE_SIZE);
                            ApiResponse<AdvertCollectCardRecord> response = advertCollectCardRecordApiService.findAll(apiRequest,apiRequestPage);
                            if (response != null && response.getPagedData() != null) {
                                sendMessage(response.getPagedData().stream().filter(advertCollectCardRecord -> advertCollectCardRecord.getCards().equals(isAllStatus)).collect(Collectors.toList()), advert.getAssetType());
                                while (response.getPagedData().size() > 0) {
                                    pageIndex++;
                                    apiRequestPage = ApiRequestPage.newInstance();
                                    apiRequestPage.paging(pageIndex,PAGE_SIZE);
                                    response = advertCollectCardRecordApiService.findAll(apiRequest,apiRequestPage);
                                    if (response != null && response.getPagedData() != null && response.getPagedData().size() > 0) {
                                        this.sendMessage(response.getPagedData().stream().filter(advertCollectCardRecord -> advertCollectCardRecord.getCards().equals(isAllStatus)).collect(Collectors.toList()),advert.getAssetType());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void sendMessage(List<AdvertDeliverRange> list, Long advertId, AssetType assetType) {
        if (list == null || list.size() == 0) {
            return;
        }
        Map<AdvertAdditionalType, String> map = advertAdditionalApiService.findAdditional(advertId, AdvertAdditionalType.PUSH);
        if (map == null || map.size() == 0) {
            logger.error("未找到推送信息, advertId = {}", advertId);
            return;
        }
        if (list != null && list.size() == 1) {
            AdvertDeliverRange advertDeliverRange = list.get(0);
            Long targetId = advertDeliverRange.getTargetId();
            //推送到全国
            Map<String,String> params = Maps.newHashMap();
            params.put("content",map.get(AdvertAdditionalType.PUSH));
            Map<String,String> forwardParams = Maps.newHashMap();
            forwardParams.put("advertId",advertId.toString());
            if (DeliverRangeType.REGION.equals(advertDeliverRange.getType()) && 100000 == targetId) {
                Boolean flag = true;
                logger.info("集卡开始推送到全国，advertId:{},params:{}",advertId,JSON.toJSONString(params));
                Long maxUserId = 0L;
                while(flag) {
                    try {
                        ApiResponse<UserInfoIndex> api = userInfoIndexApiService.findAll(ApiRequest.newInstance().filterLike(QUserInfoIndex.del, UserStatus.NotDeleted).filterGreaterThan(QUserInfoIndex.id, maxUserId), ApiRequestPage.newInstance().paging(0, JIGUANG_PUSH_MAX).addOrder(QUserInfoIndex.id, PageOrderType.ASC));
                        List<UserInfoIndex> userInfoIndexList = Lists.newArrayList(api.getPagedData());
                        if (userInfoIndexList != null && userInfoIndexList.size() > 0) {
                            List<String> clientIds = Lists.newArrayList(userInfoIndexList.stream().filter(userInfoIndex -> StringUtil.isNotEmpty(userInfoIndex.getClientId())).map(UserInfoIndex::getClientId).collect(Collectors.toSet()));
                            if (!CollectionUtils.isEmpty(clientIds)) {
                                pushMessage(clientIds, MessageKeyConstants.COLLECT_CARD_START,params,forwardParams,assetType);
                            }
                            maxUserId = userInfoIndexList.get(userInfoIndexList.size() - 1).getId();
                        } else {
                            flag = false;
                            logger.info("集卡开始推送到全国结束，advertId:{},maxUserId:{}",advertId,maxUserId);
                        }
                    }catch(Exception e){
                        logger.error("集卡开始推送查询用户数据时出错",e);
                    }
                }
            } else {
                // TODO: 2018/10/22 不是全国推送的暂时先放着
                logger.info("集卡开始推送范围不是到全国advertId:{}",advertId);
            }
        }
    }

    private void sendMessage(List<AdvertCollectCardRecord> list,AssetType assetType) {
        if (list == null || list.size() == 0) {
            return;
        }
        for (AdvertCollectCardRecord record : list) {
            try {
                Map<String, String> params = Maps.newHashMap();
                params.put("content","猴塞雷！恭喜您集齐卡片，正在开奖!");
                Map<String, String> forwardParams = Maps.newHashMap();
                forwardParams.put("advertId", record.getAdvertId().toString());
                Long userId = record.getUserId();
                UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userId);
                if (userInfoIndex == null || userInfoIndex.getClientId() == null) {
                    continue;
                }
                pushMessage(userInfoIndex.getClientId(), MessageKeyConstants.COLLECT_CARD_LOTTERY,params,forwardParams,assetType);
            } catch (Exception e) {
                logger.error("推送失败，userId = {}, advertId = {}", record.getUserId(), record.getAdvertId(), e);
            }
        }
    }

    /**
     * 批量推送消息
     * @param clientIds clientId 集合
     * @param messageKey message key
     * @param params 参数
     * @param forwardParams APP传递参数
     */
    public void pushMessage(List<String> clientIds, String messageKey, Map<String,String> params, Map<String,String> forwardParams,AssetType assetType){
        try {
            List<ClientDeviceIndex> all = clientDeviceIndexApiService.findAll(ApiRequest.newInstance().filterLikes(QClientDeviceIndex.clientId, clientIds));
            if(all == null || all .size() == 0) {
//                logger.error("1设备推送信息 未找到, clientIds = {}",JSON.toJSONString(clientIds));
                return;
            }
            all = all.stream().filter(clientDeviceIndex -> checkAppVersion(clientDeviceIndex.getClientOSType(),clientDeviceIndex.getAppVersionCode(),assetType)).collect(Collectors.toList());
            if(all == null || all .size() == 0) {
                logger.error("设备推送信息 未找到, clientIds = {}",JSON.toJSONString(clientIds));
                return;
            }
            List<String> vendorClientIds = Lists.newArrayList(all.stream().map(ClientDeviceIndex::getVendorClientId).collect(Collectors.toSet()));
            if (vendorClientIds == null || vendorClientIds.size() == 0) {
                logger.error("设备推送信息 未找到, vendorClientIds = {}", JSON.toJSONString(vendorClientIds));
                return;
            }
//            logger.info("开始批量推送集卡开始推送，vendorClientIds:{},params:{}",JSON.toJSONString(vendorClientIds),JSON.toJSONString(params));
            if (vendorClientIds.size() > JIGUANG_PUSH_MAX) {
                List<List<String>> partition = Lists.partition(vendorClientIds, JIGUANG_PUSH_MAX);
                for (List<String> subClientIds : partition) {
                    pushComponent.pushBatch(subClientIds, messageKey, params, forwardParams, PushOsType.ALL);
                }
            } else {
                pushComponent.pushBatch(vendorClientIds, messageKey, params, forwardParams, PushOsType.ALL);
            }
        } catch (Exception e) {
            logger.error(String.format("推送消息失败! clientIds = %s", JSON.toJSONString(clientIds)), e);
        }
    }

    /**
     *  推送消息
     * @param clientId clientID
     * @param messageKey message类型
     * @param params 参数
     * @param forwardParams APP传递参数
     */
    private void pushMessage(String clientId, String messageKey, Map<String, String> params, Map<String, String> forwardParams,AssetType assetType){
        try {
            ClientDevice clientDevice = deviceApiService.getClientDevice(ClientType.SQBJ, clientId);
            if (clientDevice == null) {
                logger.error("设备信息 未找到, clientId = {}", clientId);
                return;
            }
            if(!checkAppVersion(clientDevice.getClientOSType(),clientDevice.getAppVersionCode(),assetType)){
                logger.error("APP版本不符合新版集卡推送, clientId = {}， assetType:{}, clientOSType:{},appVersionCode:{}", clientId,assetType,clientDevice.getClientOSType(),clientDevice.getAppVersionCode());
                return;
            }
            PushDeviceInfo pushDeviceInfo = deviceApiService.getPushDeviceInfo(clientDevice.getId(), PushVendorType.JIGUANG);
            if (pushDeviceInfo == null) {
                logger.error("推送信息 未找到, clientId = {}", clientDevice.getId());
                return;
            }
            pushComponent.pushSingle(pushDeviceInfo.getVendorClientId(), messageKey, params, forwardParams, clientDevice.getClientOSType().equals(ClientOSType.ANDROID) ? PushOsType.ANDROID : PushOsType.IOS);
        } catch (Exception e) {
            logger.error(String.format("推送消息失败! clientId = %s", clientId), e);
        }
    }

    private Boolean checkAppVersion(ClientOSType clientOSType, Long appVersionCode, AssetType assetType){
        Boolean flag = false;
        if (ClientOSType.ANDROID.equals(clientOSType)) {
            if (appVersionCode >= 3410) {
                flag = true;
            }
        } else if (ClientOSType.IOS.equals(clientOSType)){
            if (appVersionCode >= 341) {
                flag = true;
            }
        }
        if (AssetType.MONEY.equals(assetType)) {
            flag = true;
        }
        return flag;
    }
}
