package cn.lehome.dispatcher.queue.listener.auth;

import cn.lehome.base.api.tool.bean.community.CommunityExt;
import cn.lehome.base.api.tool.bean.device.ClientDeviceIndex;
import cn.lehome.base.api.tool.bean.message.MessageTemplate;
import cn.lehome.base.api.tool.bean.push.PushSendRecord;
import cn.lehome.base.api.tool.compoment.message.push.PushComponent;
import cn.lehome.base.api.tool.constant.MessageKeyConstants;
import cn.lehome.base.api.tool.event.RemoveUserHouseEventBean;
import cn.lehome.base.api.tool.service.community.CommunityCacheApiService;
import cn.lehome.base.api.tool.service.device.ClientDeviceIndexApiService;
import cn.lehome.base.api.tool.service.device.DeviceApiService;
import cn.lehome.base.api.user.bean.message.UserMessage;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.message.UserMessageApiService;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.tool.entity.enums.auth.RemoveUserHouseType;
import cn.lehome.bean.tool.entity.enums.push.PushOsType;
import cn.lehome.bean.user.entity.enums.user.HouseType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.template.MessageTemplateService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.bean.core.enums.ClientOSType;
import cn.lehome.framework.bean.core.enums.ClientType;
import cn.lehome.framework.bean.core.enums.PushGroupType;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class RemoveUserHouseSendMessageListener extends AbstractJobListener {

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private ClientDeviceIndexApiService clientDeviceIndexApiService;

    @Autowired
    private PushComponent pushComponent;

    @Autowired
    private UserMessageApiService userMessageApiService;

    @Autowired
    private MessageTemplateService messageTemplateService;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof SimpleEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<RemoveUserHouseEventBean> simpleEventMessage = (SimpleEventMessage) eventMessage;
        RemoveUserHouseEventBean removeUserHouseEventBean = simpleEventMessage.getData();
        switch (removeUserHouseEventBean.getType()) {
            case KICK:
            case REMOVE:
                this.sndMessage(removeUserHouseEventBean.getObjectId(), removeUserHouseEventBean.getType());
                break;
            case EXIT:
            default:
                logger.error("自己退出家人圈不发推送");
                break;
        }
    }

    private void sndMessage(Long relationId, RemoveUserHouseType type) {
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
        if (userInfoIndex.getIsLogin().equals(YesNoStatus.YES)) {
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
            Map<String, String> params = Maps.newHashMap();
            params.put("communityName", communityExt.getName());
            params.put("houseAddress", userHouseRelationship.getHouseAddress());
            String messageTemplateKey = MessageKeyConstants.KICK_FAMILY_PUSH_MESSAGE;
            if (userHouseRelationship.getHouseType().equals(HouseType.RENTER)) {
                messageTemplateKey = MessageKeyConstants.KICK_RENTER_PUSH_MESSAGE;
            }
            if (RemoveUserHouseType.REMOVE.equals(type)) {
                messageTemplateKey = MessageKeyConstants.REMOVE_HOUSE_PUSH_MESSAGE;
            }

            PushSendRecord pushSendRecord = pushComponent.pushSingle(clientDeviceIndex.getVendorClientId(), messageTemplateKey, params, Maps.newHashMap(), clientDeviceIndex.getClientOSType().equals(ClientOSType.ANDROID) ? PushOsType.ANDROID : PushOsType.IOS);
            MessageTemplate messageTemplate = messageTemplateService.getMessageTemplateIdFromKey(messageTemplateKey);
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
            logger.error("用户未登录, 无需发送推动, relationId = {}", relationId);
        }
    }



    @Override
    public String getConsumerId() {
        return "remove_user_house_send_message";
    }
}
