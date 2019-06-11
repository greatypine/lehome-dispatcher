package cn.lehome.dispatcher.queue.listener.post;

import cn.lehome.base.api.business.content.bean.post.PostInfoIndex;
import cn.lehome.base.api.business.content.bean.post.RecommendInfo;
import cn.lehome.base.api.business.content.service.post.PostInfoIndexApiService;
import cn.lehome.base.api.business.content.service.post.RecommendInfoApiService;
import cn.lehome.base.api.common.bean.community.Community;
import cn.lehome.base.api.common.bean.device.ClientDeviceIndex;
import cn.lehome.base.api.common.bean.device.QClientDeviceIndex;
import cn.lehome.base.api.common.bean.message.MessageTemplate;
import cn.lehome.base.api.common.bean.push.PushSendRecord;
import cn.lehome.base.api.common.component.message.push.PushComponent;
import cn.lehome.base.api.common.constant.MessageKeyConstants;
import cn.lehome.base.api.common.service.community.CommunityCacheApiService;
import cn.lehome.base.api.common.service.device.ClientDeviceIndexApiService;
import cn.lehome.base.api.user.bean.message.UserMessage;
import cn.lehome.base.api.user.bean.user.QUserInfoIndex;
import cn.lehome.base.api.user.bean.user.UserCommunityRelationship;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.message.UserMessageApiService;
import cn.lehome.base.api.user.service.user.UserCommunityRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.template.MessageTemplateService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.exception.sql.NotFoundRecordException;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.bean.core.enums.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by zhanghuan on 18/5/14.
 */
public class RecommendPostListener extends AbstractJobListener {

    @Autowired
    private RecommendInfoApiService recommendInfoApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private ClientDeviceIndexApiService clientDeviceIndexApiService;

    @Autowired
    private PushComponent pushComponent;

    @Autowired
    private UserCommunityRelationshipApiService userCommunityRelationshipApiService;

    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private PostInfoIndexApiService postInfoIndexApiService;

    @Autowired
    private UserMessageApiService userMessageApiService;

    @Autowired
    private MessageTemplateService messageTemplateService;

    private static final Double  RECOMMEND_RADIUS = 1000.00;

    private static final Integer ES_MAXCLAUSECOUNT = 1024;

    private static final Integer PUSH_CLIENT_MAXSIZE=500;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        RecommendInfo recommendInfo = recommendInfoApiService.get(longEventMessage.getData());
        if (recommendInfo == null) {
            logger.error("推荐帖子文案任务未找到, recommendId = {}", longEventMessage.getData());
            return;
        }
        recommendInfo.setSentStatus(SendStatus.DEALING);
        recommendInfo = recommendInfoApiService.update(recommendInfo);
        boolean isDeal = true;
        try {
            Long communityId = recommendInfo.getCommunityId();
            List<Long> communityRelationUserIds = Lists.newArrayList();
            List<Long> houseRelationUserIds = Lists.newArrayList();
            List<UserCommunityRelationship> userCommunityRelationshipList = userCommunityRelationshipApiService.findByCommunityId(communityId);
            if (!CollectionUtils.isEmpty(userCommunityRelationshipList)) {
                communityRelationUserIds = userCommunityRelationshipList.stream().map(UserCommunityRelationship::getUserId).collect(Collectors.toList());
            }
            Community community = communityCacheApiService.getCommunity(communityId);
            if (community == null) {
                throw new NotFoundRecordException();
            }
            List<UserHouseRelationship> userHouseRelationships = userHouseRelationshipApiService.findByCommunityExtIdAndEnableStatus(community.getCommunityExtId(), EnableDisableStatus.ENABLE);
            if (!CollectionUtils.isEmpty(userHouseRelationships)) {
                houseRelationUserIds = userHouseRelationships.stream().map(UserHouseRelationship::getUserId).collect(Collectors.toList());
            }
            communityRelationUserIds.addAll(houseRelationUserIds);

            if(YesNoStatus.NO.equals(recommendInfo.getIsLocalRange())){
                String[] str = community.getLocation().split(",");
                Double longitude = Double.valueOf(str[0]);
                Double latitude = Double.valueOf(str[1]);
                List<Long> aroudCommunityIds = communityCacheApiService.findCommunityIdByGeoRadius(longitude, latitude, RECOMMEND_RADIUS);
                logger.error("周围1公里小区ID集合 ids = {}, radius = {}", StringUtils.join(aroudCommunityIds, ","), RECOMMEND_RADIUS);
                if(!CollectionUtils.isEmpty(aroudCommunityIds)){
                    Map<Long, Community> aroundCommunity = communityCacheApiService.findAllCommunity(aroudCommunityIds);
                    List<Community> communities =  aroundCommunity.entrySet().stream().map(x ->x.getValue()).collect(Collectors.toList());
                    List<Long> aroundCommunityExtIds = communities.stream().map(Community::getCommunityExtId).collect(Collectors.toList());
                    List<UserCommunityRelationship> aroundCommunityRelationshipList = userCommunityRelationshipApiService.findByCommunityIds(aroudCommunityIds);
                    List<UserHouseRelationship> aroundHouseRelationships = userHouseRelationshipApiService.findByCommunityExtIdsAndEnableStatus(aroundCommunityExtIds,EnableDisableStatus.ENABLE);
                    if(!CollectionUtils.isEmpty(aroundCommunityRelationshipList)){
                        List<Long> aroundHouseRelationUserIds = aroundCommunityRelationshipList.stream().map(UserCommunityRelationship::getUserId).collect(Collectors.toList());
                        communityRelationUserIds.addAll(aroundHouseRelationUserIds);
                    }
                    if(!CollectionUtils.isEmpty(aroundHouseRelationships)){
                        List<Long> aroundHouseRelationUserIds = aroundHouseRelationships.stream().map(UserHouseRelationship::getUserId).collect(Collectors.toList());
                        communityRelationUserIds.addAll(aroundHouseRelationUserIds);
                    }
                }
            }
            String postId = recommendInfo.getPostId();
            String content = recommendInfo.getContent();
            PostInfoIndex postInfoIndex =  postInfoIndexApiService.get(postId);
            Long userId = postInfoIndex.getUserId();
            Map<String,String> forwardParams = Maps.newHashMap();
            forwardParams.put("value",postId);
            forwardParams.put("postId",postId);
            String vendorClientId = "";
            if(postInfoIndex !=null && (!postInfoIndex.getUserType().equals(cn.lehome.bean.business.content.enums.post.UserType.ROBOT))){
                vendorClientId = singlePostPush(userId,forwardParams);
                logger.error("vendorClientId为:"+vendorClientId);
            }
            if(CollectionUtils.isNotEmpty(communityRelationUserIds)){
                List<Long> distinctUserIds = communityRelationUserIds.stream().distinct().collect(Collectors.toList());
                logger.error("distinctUserIds的大小为: "+distinctUserIds.size());
                if(distinctUserIds.size()>ES_MAXCLAUSECOUNT){
                    List<List<Long>> subUserLists = Lists.partition(distinctUserIds,ES_MAXCLAUSECOUNT);
                    for (List<Long> userIdList : subUserLists){
                        batchPushPost(userIdList,userId,vendorClientId,forwardParams,content);
                    }
                }else{
                    batchPushPost(distinctUserIds,userId,vendorClientId,forwardParams,content);
                }

            }
        }catch (Exception e){
            logger.error("推荐帖子文案失败", e.getMessage());
            isDeal = false;
        }finally {
            if(isDeal){
                recommendInfo.setSentStatus(SendStatus.DEAL);
                recommendInfoApiService.update(recommendInfo);
            }else {
                recommendInfo.setSentStatus(SendStatus.DEAL_FAILED);
                recommendInfoApiService.update(recommendInfo);
            }
        }
    }


    private void batchPushPost(List<Long> userIdList,Long userId,String vendorClientId,Map<String,String> forwardParams,String content){
        Set<String> vendorClientIds = getVendorClientIds(userIdList);
        if(!vendorClientIds.isEmpty()){
            Iterator<String> iterator = vendorClientIds.iterator();
            while (iterator.hasNext()){
                if(iterator.next().equals(vendorClientId)){
                    iterator.remove();
                }
            }
            logger.error("开始批量推送帖子文案信息 vendorClientIds大小为"+vendorClientIds.size()+"\n vendorClientIds元素为:"+StringUtils.join(vendorClientIds,","));
            List<String> clientIdsList =  Lists.newArrayList(vendorClientIds);
            Map<String,String> contentParam = Maps.newHashMap();
            contentParam.put("content",content);
            if(clientIdsList.size()>PUSH_CLIENT_MAXSIZE){
                List<List<String>> subCliendIds = Lists.partition(clientIdsList,PUSH_CLIENT_MAXSIZE);
                for(List<String> cliendIds : subCliendIds){
                    PushSendRecord pushSendRecord = pushComponent.pushBatch(cliendIds, MessageKeyConstants.RECOMMEND_POST,contentParam, forwardParams, PushOsType.ALL);

                    MessageTemplate messageTemplate = messageTemplateService.getMessageTemplateIdFromKey(MessageKeyConstants.RECOMMEND_POST);
                    if (messageTemplate.getIsContainsStationLetter().equals(YesNoStatus.YES)){
                        saveBatchUserMessage(userIdList,userId,pushSendRecord,messageTemplate.getId());
                    }
                }
            }else{
                PushSendRecord pushSendRecord = pushComponent.pushBatch(clientIdsList, MessageKeyConstants.RECOMMEND_POST,contentParam, forwardParams, PushOsType.ALL);

                MessageTemplate messageTemplate = messageTemplateService.getMessageTemplateIdFromKey(MessageKeyConstants.RECOMMEND_POST);
                if (messageTemplate.getIsContainsStationLetter().equals(YesNoStatus.YES)){
                    saveBatchUserMessage(userIdList,userId,pushSendRecord,messageTemplate.getId());
                }
            }
        }
    }


    private void saveBatchUserMessage(List<Long> distinctUserIds, Long singlePushUserId, PushSendRecord pushSendRecord,Long messageTemplateId) {
        Iterator<Long> iterator = distinctUserIds.iterator();
        while (iterator.hasNext()){
            if (iterator.next().equals(singlePushUserId)){
                iterator.remove();
            }
        }
        List<UserMessage> userMessages = Lists.newArrayList();
        for (Long userid : distinctUserIds) {
            UserMessage userMessage = new UserMessage();
            userMessage.setPushGroupType(PushGroupType.SYSTEM);
            userMessage.setPushHistoryRecordId(pushSendRecord.getMessageId());
            userMessage.setUserId(userid);
            userMessage.setContent(pushSendRecord.getContent());
            userMessage.setMessageTemplateId(messageTemplateId);
            userMessages.add(userMessage);
        }
        userMessageApiService.saveBatch(userMessages);
    }

    private String singlePostPush(Long userId,Map<String, String> forwardParams) {
        UserInfoIndex userIndex = userInfoIndexApiService.findByUserId(userId);
        String vendorClientId = "";
        if (userIndex != null) {
            if (StringUtils.isNotEmpty(userIndex.getClientId())) {
                ClientDeviceIndex clientDeviceIndex = clientDeviceIndexApiService.get(userIndex.getClientId(), ClientType.SQBJ);
                if (clientDeviceIndex != null && StringUtils.isNotEmpty(clientDeviceIndex.getVendorClientId())) {
                    Map<String, String> params = Maps.newHashMap();
                    params.put("content", "恭喜您发布的内容已经被推送周边小区论坛！半径名人就是你！");
                    try {
                        vendorClientId = clientDeviceIndex.getVendorClientId();
                        PushSendRecord pushSendRecord = pushComponent.pushSingle(vendorClientId, MessageKeyConstants.RECOMMEND_POST, params, forwardParams, clientDeviceIndex.getClientOSType().equals(ClientOSType.IOS) ? PushOsType.IOS : PushOsType.ANDROID);

                        MessageTemplate messageTemplate = messageTemplateService.getMessageTemplateIdFromKey(MessageKeyConstants.RECOMMEND_POST);
                        if (messageTemplate.getIsContainsStationLetter().equals(YesNoStatus.YES)){
                            UserMessage userMessage = new UserMessage();
                            userMessage.setUserId(userId);
                            userMessage.setPushGroupType(PushGroupType.SYSTEM);
                            userMessage.setPushHistoryRecordId(pushSendRecord.getId());
                            userMessage.setContent(pushSendRecord.getContent());
                            userMessage.setMessageTemplateId(messageTemplate.getId());
                            List<UserMessage> userMessageList = Lists.newArrayList();
                            userMessageList.add(userMessage);
                            userMessageApiService.saveBatch(userMessageList);
                        }
                    } catch (Exception e) {
                        logger.error("推送失败", e);
                    }
                }
            }
        }
        return vendorClientId;
    }


    private Set<String> getVendorClientIds(List<Long> userIds){
        List<UserInfoIndex> userInfoIndexList = userInfoIndexApiService.findAll(ApiRequest.newInstance().filterIn(QUserInfoIndex.id, userIds));
        if (org.springframework.util.CollectionUtils.isEmpty(userInfoIndexList)) {
            return new HashSet<>();
        }
        userInfoIndexList = userInfoIndexList.stream().filter(userInfoIndex -> StringUtils.isNotEmpty(userInfoIndex.getClientId())).collect(Collectors.toList());
        if (org.springframework.util.CollectionUtils.isEmpty(userInfoIndexList)) {
            return new HashSet<>();
        }
        Set<String> clientIds = userInfoIndexList.stream().map(UserInfoIndex::getClientId).collect(Collectors.toSet());
        logger.error("clientIds 的大小为: "+clientIds.size());
        List<ClientDeviceIndex>  clientDeviceIndexs = clientDeviceIndexApiService.findAll(ApiRequest.newInstance().filterLikes(QClientDeviceIndex.clientId, clientIds));
        if (org.springframework.util.CollectionUtils.isEmpty(clientDeviceIndexs)) {
            return new HashSet<>();
        }
        clientDeviceIndexs = clientDeviceIndexs.stream().filter(clientDeviceIndex -> StringUtils.isNotEmpty(clientDeviceIndex.getClientId())).collect(Collectors.toList());
        if (org.springframework.util.CollectionUtils.isEmpty(clientDeviceIndexs)) {
            return new HashSet<>();
        }
        clientDeviceIndexs = clientDeviceIndexs.stream().filter(clientDeviceIndex -> StringUtils.isNotEmpty(clientDeviceIndex.getVendorClientId())).collect(Collectors.toList());
        Set<String> vendorClientIds = clientDeviceIndexs.stream().map(ClientDeviceIndex::getVendorClientId).collect(Collectors.toSet());
        return vendorClientIds;
    }

    @Override
    public String getConsumerId() {
        return "recommend_post";
    }
}
