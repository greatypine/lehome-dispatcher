package cn.lehome.dispatcher.queue.listener.login;

import cn.lehome.base.api.tool.bean.device.ClientDevice;
import cn.lehome.base.api.tool.bean.device.PushDeviceInfo;
import cn.lehome.base.api.tool.bean.message.MessageTemplate;
import cn.lehome.base.api.tool.bean.push.PushSendRecord;
import cn.lehome.base.api.tool.compoment.message.push.PushComponent;
import cn.lehome.base.api.tool.constant.MessageKeyConstants;
import cn.lehome.base.api.tool.service.device.DeviceApiService;
import cn.lehome.base.api.user.bean.message.UserMessage;
import cn.lehome.base.api.user.bean.user.UserDeviceRelationship;
import cn.lehome.base.api.user.service.message.UserMessageApiService;
import cn.lehome.base.api.user.service.user.UserDeviceRelationshipApiService;
import cn.lehome.bean.tool.entity.enums.device.PushVendorType;
import cn.lehome.bean.tool.entity.enums.push.PushOsType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.template.MessageTemplateService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.bean.core.enums.ClientOSType;
import cn.lehome.framework.bean.core.enums.PushGroupType;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class KickOtherDeviceLoginUserListener extends AbstractJobListener {

    @Autowired
    private UserDeviceRelationshipApiService userDeviceRelationshipApiService;

    @Autowired
    private DeviceApiService deviceApiService;

    @Autowired
    private PushComponent pushComponent;

    @Autowired
    private UserMessageApiService userMessageApiService;

    @Autowired
    private MessageTemplateService messageTemplateService;


    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof  LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        Long userDeviceRelationshipId = longEventMessage.getData();
        UserDeviceRelationship userDeviceRelationship = userDeviceRelationshipApiService.get(userDeviceRelationshipId);
        if (userDeviceRelationship == null) {
            logger.error("未找到用户与设备关系信息, userDeviceRelationshipId = {}", userDeviceRelationshipId);
            return;
        }
        ClientDevice clientDevice = deviceApiService.getClientDevice(userDeviceRelationship.getClientType(), userDeviceRelationship.getClientId());
        if (clientDevice == null) {
            logger.error("设备信息未找到, clientType = {}, clientId = {}", userDeviceRelationship.getClientType(), userDeviceRelationship.getClientId());
            return;
        }
        PushDeviceInfo pushDeviceInfo = deviceApiService.getPushDeviceInfo(clientDevice.getId(), PushVendorType.JIGUANG);
        if (pushDeviceInfo == null) {
            logger.error("推送信息未找到, clientId = {}", clientDevice.getId());
            return;
        }
        PushSendRecord pushSendRecord = pushComponent.pushSilent(pushDeviceInfo.getVendorClientId(), MessageKeyConstants.LOGOUT_PUSH, Maps.newHashMap(), Maps.newHashMap(), clientDevice.getClientOSType().equals(ClientOSType.ANDROID) ? PushOsType.ANDROID : PushOsType.IOS);
        MessageTemplate messageTemplate = messageTemplateService.getMessageTemplateIdFromKey(MessageKeyConstants.LOGOUT_PUSH);
        if (messageTemplate.getIsContainsStationLetter().equals(YesNoStatus.YES)){
            UserMessage userMessage = new UserMessage();
            userMessage.setUserId(userDeviceRelationship.getUserId());
            userMessage.setPushGroupType(PushGroupType.SYSTEM);
            userMessage.setPushHistoryRecordId(pushSendRecord.getId());
            userMessage.setContent(pushSendRecord.getContent());
            userMessage.setMessageTemplateId(messageTemplate.getId());
            List<UserMessage> userMessageList = Lists.newArrayList();
            userMessageList.add(userMessage);
            userMessageApiService.saveBatch(userMessageList);
        }
    }

    @Override
    public String getConsumerId() {
        return "push_logout_message";
    }
}
