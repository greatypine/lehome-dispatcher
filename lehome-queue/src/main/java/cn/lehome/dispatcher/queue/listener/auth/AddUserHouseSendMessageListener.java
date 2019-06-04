package cn.lehome.dispatcher.queue.listener.auth;

import cn.lehome.base.api.common.bean.community.CommunityExt;
import cn.lehome.base.api.common.bean.device.ClientDeviceIndex;
import cn.lehome.base.api.common.bean.message.MessageTemplate;
import cn.lehome.base.api.common.bean.push.PushSendRecord;
import cn.lehome.base.api.common.component.message.push.PushComponent;
import cn.lehome.base.api.common.constant.MessageKeyConstants;
import cn.lehome.base.api.common.event.AddUserHouseEventBean;
import cn.lehome.base.api.common.service.community.CommunityCacheApiService;
import cn.lehome.base.api.common.service.device.ClientDeviceIndexApiService;
import cn.lehome.base.api.common.service.device.DeviceApiService;
import cn.lehome.base.api.common.service.message.MessageSendApiService;
import cn.lehome.base.api.property.bean.house.HouseDetailInfo;
import cn.lehome.base.api.property.service.house.HouseInfoApiService;
import cn.lehome.base.api.user.bean.auth.UserIdentifyChange;
import cn.lehome.base.api.user.bean.message.UserMessage;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.auth.UserIdentifyChangeApiService;
import cn.lehome.base.api.user.service.message.UserMessageApiService;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.user.entity.enums.user.HouseType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.template.MessageTemplateService;
import cn.lehome.framework.base.api.core.constant.HeaderKeyContants;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.exception.sql.NotFoundRecordException;
import cn.lehome.framework.base.api.core.service.AuthorizationService;
import cn.lehome.framework.bean.core.enums.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class AddUserHouseSendMessageListener extends AbstractJobListener {

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private DeviceApiService deviceApiService;

    @Autowired
    private PushComponent pushComponent;

    @Autowired
    private UserMessageApiService userMessageApiService;

    @Autowired
    private HouseInfoApiService houseInfoApiService;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private MessageSendApiService messageSendApiService;

    @Autowired
    private ClientDeviceIndexApiService clientDeviceIndexApiService;

    @Autowired
    private UserIdentifyChangeApiService userIdentifyChangeApiService;

    @Autowired
    private MessageTemplateService messageTemplateService;


    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof SimpleEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<AddUserHouseEventBean> simpleEventMessage = (SimpleEventMessage) eventMessage;
        AddUserHouseEventBean addUserHouseEventBean = simpleEventMessage.getData();
        switch (addUserHouseEventBean.getType()) {
            case DREW_FAMILY:
            case DREW_RENTER:
            {
                this.sendDrewUser(addUserHouseEventBean.getObjectId(), addUserHouseEventBean.getDrewUserOpenId());
                this.sendOtherUser(addUserHouseEventBean.getObjectId(), addUserHouseEventBean.getDrewUserOpenId());
            }
            break;
            case IDENTIFY_AUTH:
                this.sendIdentifyAuth(addUserHouseEventBean.getObjectId());
                break;
            case IDENTIFY_AUTH_FAILED:
                this.sendIdentifyAuthFailed(addUserHouseEventBean.getObjectId());
                break;
            case PROPERTY_AUTH:
            case LOGIN_AUTO:
            case ACTIVE_AUTH:
                break;
            default:
                    break;

        }

    }

    private void sendIdentifyAuthFailed(Long identifyId) {
        UserIdentifyChange userIdentifyChange = userIdentifyChangeApiService.get(identifyId);
        if (userIdentifyChange == null) {
            logger.error("认证申请记录未找到, identifyId = {}", identifyId);
            return;
        }
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userIdentifyChange.getUserId());
        if (userInfoIndex == null) {
            logger.error("用户信息未找到, userId = {}", userIdentifyChange.getUserId());
            return;
        }
        if (StringUtils.isEmpty(userInfoIndex.getClientId())) {
            logger.error("推送设备未找到");
            return;
        }
        ClientDeviceIndex clientDeviceIndex = clientDeviceIndexApiService.get(userInfoIndex.getClientId(), ClientType.SQBJ);
        if (clientDeviceIndex == null) {
            logger.error("设备信息未找到, clientId = {}", userInfoIndex.getClientId());
            return;
        }
        if (StringUtils.isEmpty(clientDeviceIndex.getVendorClientId())) {
            logger.error("设备信息未找到, clientId = {}", userInfoIndex.getClientId());
            return;
        }

        CommunityExt communityExt = communityCacheApiService.getCommunityExt(userIdentifyChange.getCommunityExtId());
        String applyInfo = "业主";
        if (userIdentifyChange.getHouseType().equals(HouseType.HOME)) {
            applyInfo = "我是业主家人";
        } else if (userIdentifyChange.getHouseType().equals(HouseType.RENTER)) {
            applyInfo = "租户";
        }
        Map<String, String> params = Maps.newHashMap();
        params.put("applyInfo", applyInfo);
        params.put("communityName", communityExt.getName());
        params.put("houseAddress", userIdentifyChange.getHouseInfo());
        params.put("reason", userIdentifyChange.getReason());
        Map<String, String> forwardParams = Maps.newHashMap();
        PushSendRecord pushSendRecord = pushComponent.pushSingle(clientDeviceIndex.getVendorClientId(), MessageKeyConstants.AUTH_HOUSE_FAILED, params, forwardParams, clientDeviceIndex.getClientOSType().equals(ClientOSType.ANDROID) ? PushOsType.ANDROID : PushOsType.IOS);

        MessageTemplate messageTemplate = messageTemplateService.getMessageTemplateIdFromKey(MessageKeyConstants.AUTH_HOUSE_FAILED);
        if (messageTemplate.getIsContainsStationLetter().equals(YesNoStatus.YES)){
            UserMessage userMessage = new UserMessage();
            userMessage.setUserId(userIdentifyChange.getUserId());
            userMessage.setPushGroupType(PushGroupType.SYSTEM);
            userMessage.setPushHistoryRecordId(pushSendRecord.getId());
            userMessage.setContent(pushSendRecord.getContent());
            userMessage.setMessageTemplateId(messageTemplate.getId());
            List<UserMessage> userMessageList = Lists.newArrayList();
            userMessageList.add(userMessage);
            userMessageApiService.saveBatch(userMessageList);
        }
    }

    private void sendIdentifyAuth(Long relationId) {
        UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.get(relationId);
        if (userHouseRelationship == null) {
            logger.error("房产关系记录未找到, relationId = {}", relationId);
            return;
        }
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userHouseRelationship.getUserId());
        if (userInfoIndex == null) {
            logger.error("用户信息未找到, userId = {}", userHouseRelationship.getUserId());
            return;
        }
        if (StringUtils.isEmpty(userInfoIndex.getClientId())) {
            logger.error("推送设备未找到");
            return;
        }
        ClientDeviceIndex clientDeviceIndex = clientDeviceIndexApiService.get(userInfoIndex.getClientId(), ClientType.SQBJ);
        if (clientDeviceIndex == null) {
            logger.error("设备信息未找到, clientId = {}", userInfoIndex.getClientId());
            return;
        }
        if (StringUtils.isEmpty(clientDeviceIndex.getVendorClientId())) {
            logger.error("设备信息未找到, clientId = {}", userInfoIndex.getClientId());
            return;
        }

        CommunityExt communityExt = communityCacheApiService.getCommunityExt(userHouseRelationship.getCommunityExtId());
        String applyInfo = "业主";
        if (userHouseRelationship.getHouseType().equals(HouseType.HOME)) {
            applyInfo = "我是业主家人";
        } else if (userHouseRelationship.getHouseType().equals(HouseType.RENTER)) {
            applyInfo = "租户";
        }
        Map<String, String> params = Maps.newHashMap();
        params.put("applyInfo", applyInfo);
        params.put("communityName", communityExt.getName());
        params.put("houseAddress", userHouseRelationship.getHouseAddress());
        Map<String, String> forwardParams = Maps.newHashMap();
        PushSendRecord pushSendRecord = pushComponent.pushSingle(clientDeviceIndex.getVendorClientId(), MessageKeyConstants.AUTH_HOUSE_SUCCESS, params, forwardParams, clientDeviceIndex.getClientOSType().equals(ClientOSType.ANDROID) ? PushOsType.ANDROID : PushOsType.IOS);

        MessageTemplate  messageTemplate = messageTemplateService.getMessageTemplateIdFromKey(MessageKeyConstants.AUTH_HOUSE_SUCCESS);
        if (messageTemplate.getIsContainsStationLetter().equals(YesNoStatus.YES)){
            UserMessage userMessage = new UserMessage();
            userMessage.setUserId(userHouseRelationship.getUserId());
            userMessage.setPushGroupType(PushGroupType.SYSTEM);
            userMessage.setPushHistoryRecordId(pushSendRecord.getId());
            userMessage.setContent(pushSendRecord.getContent());
            userMessage.setMessageTemplateId(messageTemplate.getId());
            List<UserMessage> userMessageList = Lists.newArrayList();
            userMessageList.add(userMessage);
            userMessageApiService.saveBatch(userMessageList);
        }
    }

    private void sendDrewUser(Long relationId, String userOpenId) {
        UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.get(relationId);
        if (userHouseRelationship == null) {
            throw new NotFoundRecordException(String.format("未找到用户认证数据, relationId = %s", relationId));
        }
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userHouseRelationship.getUserId());
        if (userInfoIndex == null) {
            throw new NotFoundRecordException(String.format("用户索引数据未找到, userId = %s", userHouseRelationship.getUserId()));
        }
        String apiToken = authorizationService.getApiToken(userOpenId);
        if (StringUtils.isEmpty(apiToken)) {
            logger.error("apiToken未找到");
            return;
        }
        CommunityExt communityExt = communityCacheApiService.getCommunityExt(userHouseRelationship.getCommunityExtId());
        Map<String, String> headerMap = Maps.newHashMap();
        headerMap.put(HeaderKeyContants.API_CLIENT_ID, "");
        headerMap.put(HeaderKeyContants.API_APP_ID, "");
        headerMap.put(HeaderKeyContants.API_PRO_APP_OAUTH_TOKEN, apiToken);
        headerMap.put(HeaderKeyContants.EDITION_TYPE_KEY, communityExt.getEditionType().toString());
        HouseDetailInfo houseDetailInfo = houseInfoApiService.findOne(userHouseRelationship.getHouseId(), headerMap);
        if (houseDetailInfo == null) {
            logger.error("未找到房产详情, houseId = {}, editionType = {}", userHouseRelationship.getHouseId(), communityExt.getEditionType());
            return;
        }
        Map<String, String> params = Maps.newHashMap();
        params.put("houseOwnerName", houseDetailInfo.getHouseOwnerName());
        params.put("communityName", communityExt.getName());
        params.put("houseAddress", houseDetailInfo.getAddress());
        params.put("downloadUrl", "");
        String pushKey = MessageKeyConstants.INVITED_FAMILY_PUSH_MESSAGE;
        String smsKey = MessageKeyConstants.INVITED_FAMILY_SMS;
        if (userHouseRelationship.getHouseType().equals(HouseType.RENTER)) {
            pushKey = MessageKeyConstants.INVITED_RENTER_PUSH_MESSAGE;
            smsKey = MessageKeyConstants.INVITED_RENTER_SMS;
        }

        if (userInfoIndex.getIsLogin().equals(YesNoStatus.YES)) {
            //已经登录用户, 推送消息
            if (StringUtils.isEmpty(userInfoIndex.getClientId())) {
                logger.error("推送设备未找到");
                return;
            }
            ClientDeviceIndex clientDeviceIndex = clientDeviceIndexApiService.get(userInfoIndex.getClientId(), ClientType.SQBJ);
            if (clientDeviceIndex == null) {
                logger.error("设备信息未找到, clientId = {}", userInfoIndex.getClientId());
                return;
            }
            if (StringUtils.isEmpty(clientDeviceIndex.getVendorClientId())) {
                logger.error("设备信息未找到, clientId = {}", userInfoIndex.getClientId());
                return;
            }
            Map<String, String> forwardParamsMap = Maps.newHashMap();
            forwardParamsMap.put("relationId", userHouseRelationship.getId().toString());
            forwardParamsMap.put("communityExtId", userHouseRelationship.getCommunityExtId().toString());
            forwardParamsMap.put("edition", communityExt.getEditionType().toString());
            PushSendRecord pushSendRecord = pushComponent.pushSingle(clientDeviceIndex.getVendorClientId(), pushKey, params, forwardParamsMap, clientDeviceIndex.getClientOSType().equals(ClientOSType.ANDROID) ? PushOsType.ANDROID : PushOsType.IOS);

            MessageTemplate messageTemplate = messageTemplateService.getMessageTemplateIdFromKey(pushKey);
            if (messageTemplate.getIsContainsStationLetter().equals(YesNoStatus.YES)){
                UserMessage userMessage = new UserMessage();
                userMessage.setUserId(userHouseRelationship.getUserId());
                userMessage.setPushGroupType(PushGroupType.SYSTEM);
                userMessage.setPushHistoryRecordId(pushSendRecord.getId());
                userMessage.setContent(pushSendRecord.getContent());
                userMessage.setMessageTemplateId(messageTemplate.getId());
                List<UserMessage> userMessageList = Lists.newArrayList();
                userMessageList.add(userMessage);
                userMessageApiService.saveBatch(userMessageList);
            }

        } else {
            //未登录用户, 发送短信
            messageSendApiService.sendSingleSms(userInfoIndex.getPhone(), smsKey, params);
        }
    }

    private void sendOtherUser(Long relationId, String userOpenId) {
        UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.get(relationId);
        if (userHouseRelationship == null) {
            throw new NotFoundRecordException(String.format("未找到用户认证数据, relationId = %s", relationId));
        }
        UserInfoIndex drewUserInfoIndex = userInfoIndexApiService.findByOpenId(userOpenId);
        if (drewUserInfoIndex == null) {
            throw new NotFoundRecordException(String.format("用户索引数据未找到, userOpenId = %s", userOpenId));
        }
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userHouseRelationship.getUserId());
        if (userInfoIndex == null) {
            throw new NotFoundRecordException(String.format("用户索引数据未找到, userId = %s", userHouseRelationship.getUserId()));
        }
        List<UserHouseRelationship> userHouseRelationshipList = userHouseRelationshipApiService.findByHouse(userHouseRelationship.getCommunityExtId(), userHouseRelationship.getHouseId());
        List<UserHouseRelationship> otherUserHouseList = Lists.newArrayList();
        for (UserHouseRelationship relationship : userHouseRelationshipList) {
            if (relationship.getHouseType().equals(HouseType.FORBID) || relationship.getHouseType().equals(HouseType.RENTER)) {
                continue;
            }
            if (relationship.getEnableStatus().equals(EnableDisableStatus.DISABLE)) {
                continue;
            }
            if (relationship.getUserId().equals(drewUserInfoIndex.getId()) || relationship.getUserId().equals(userHouseRelationship.getUserId())) {
                continue;
            }
            otherUserHouseList.add(relationship);
        }
        if (!CollectionUtils.isEmpty(otherUserHouseList)) {
            for (UserHouseRelationship relationship : otherUserHouseList) {
                UserInfoIndex userIndex = userInfoIndexApiService.findByUserId(relationship.getUserId());
                if (userIndex == null) {
                    logger.error("用户信息未找到, userId = {}", relationship.getUserId());
                    continue;
                }
                if (StringUtils.isEmpty(userIndex.getClientId())) {
                    logger.error("用户设备信息未找到, userId = {}", relationship.getUserId());
                    continue;
                }
                ClientDeviceIndex clientDeviceIndex = clientDeviceIndexApiService.get(userIndex.getClientId(), ClientType.SQBJ);
                if (clientDeviceIndex == null) {
                    logger.error("用户设备信息未找到, userId = {}", relationship.getUserId());
                    continue;
                }
                if (StringUtils.isEmpty(clientDeviceIndex.getVendorClientId())) {
                    logger.error("用户设备推送信息未找到, userId = {}", relationship.getUserId());
                    continue;
                }
                CommunityExt communityExt = communityCacheApiService.getCommunityExt(userHouseRelationship.getCommunityExtId());
                Map<String, String> params = Maps.newHashMap();
                params.put("communityName", communityExt.getName());
                params.put("houseAddress", userHouseRelationship.getHouseAddress());
                Map<String, String> forwardParamsMap = Maps.newHashMap();
                forwardParamsMap.put("relationId", userHouseRelationship.getId().toString());
                forwardParamsMap.put("communityExtId", userHouseRelationship.getCommunityExtId().toString());
                forwardParamsMap.put("edition", communityExt.getEditionType().toString());
                PushSendRecord pushSendRecord = pushComponent.pushSingle(clientDeviceIndex.getVendorClientId(), MessageKeyConstants.OTHER_FAMILY_PUSH_MESSAGE, params, forwardParamsMap, clientDeviceIndex.getClientOSType().equals(ClientOSType.ANDROID) ? PushOsType.ANDROID : PushOsType.IOS);
                MessageTemplate messageTemplate = messageTemplateService.getMessageTemplateIdFromKey(MessageKeyConstants.OTHER_FAMILY_PUSH_MESSAGE);
                if (messageTemplate.getIsContainsStationLetter().equals(YesNoStatus.YES)){
                    List<UserMessage> userMessageList = Lists.newArrayList();
                    UserMessage userMessage = new UserMessage();
                    userMessage.setUserId(relationship.getUserId());
                    userMessage.setPushGroupType(PushGroupType.SYSTEM);
                    userMessage.setPushHistoryRecordId(pushSendRecord.getId());
                    userMessage.setContent(pushSendRecord.getContent());
                    userMessage.setMessageTemplateId(messageTemplate.getId());
                    userMessageList.add(userMessage);
                    userMessageApiService.saveBatch(userMessageList);
                }
            }
        }
    }


    @Override
    public String getConsumerId() {
        return "add_user_house_send_message";
    }
}
