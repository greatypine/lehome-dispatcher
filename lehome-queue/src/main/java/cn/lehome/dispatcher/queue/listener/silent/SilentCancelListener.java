package cn.lehome.dispatcher.queue.listener.silent;

import cn.lehome.base.api.common.bean.device.ClientDeviceIndex;
import cn.lehome.base.api.common.bean.message.MessageTemplate;
import cn.lehome.base.api.common.bean.push.PushSendRecord;
import cn.lehome.base.api.common.component.message.push.PushComponent;
import cn.lehome.base.api.common.constant.MessageKeyConstants;
import cn.lehome.base.api.common.service.device.ClientDeviceIndexApiService;
import cn.lehome.base.api.common.service.message.MessageTemplateApiService;
import cn.lehome.base.api.content.bean.silent.UserSilent;
import cn.lehome.base.api.content.service.silent.UserSilentApiService;
import cn.lehome.base.api.user.bean.message.UserMessage;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.message.UserMessageApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.content.entity.enums.post.DataStatus;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.util.CoreDateUtils;
import cn.lehome.framework.bean.core.enums.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by zhuhai on 2018/5/23.
 */
public class SilentCancelListener extends AbstractJobListener {


    @Autowired
    private UserSilentApiService userSilentApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private ClientDeviceIndexApiService clientDeviceIndexApiService;

    @Autowired
    private PushComponent pushComponent;

    @Autowired
    private MessageTemplateApiService messageTemplateApiService;

    @Autowired
    private UserMessageApiService userMessageApiService;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        Long userId = longEventMessage.getData();

        UserSilent userSilent = userSilentApiService.findByUserId(userId);
        if(userSilent == null) {
            logger.error("用户禁言未找到, userId = {}", longEventMessage.getData());
        }else {
            userSilent.setDataStatus(DataStatus.DELETE);
            userSilent.setScheduleJobId(0L);
            userSilentApiService.update(userSilent);
        }
        //发送推送
        this.sendPush(userId, MessageKeyConstants.CANCEL_USER_SILENT, null, null);
    }

    @Override
    public String getConsumerId() {
        return "silent_cancel";
    }

    private void sendPush(Long userId, String messageTemplateKey,String reason,Date silentDateEnd) {
        UserInfoIndex userIndex = userInfoIndexApiService.findByUserId(userId);
        if (userIndex != null) {
            if (StringUtils.isNotEmpty(userIndex.getClientId())) {
                ClientDeviceIndex clientDeviceIndex = clientDeviceIndexApiService.get(userIndex.getClientId(), ClientType.SQBJ);
                if (clientDeviceIndex != null && StringUtils.isNotEmpty(clientDeviceIndex.getVendorClientId())) {
                    Map<String, String> params = Maps.newHashMap();
                    if(MessageKeyConstants.USER_SILENT.equals(messageTemplateKey)) {
                        if(StringUtils.isNotEmpty(reason)) {
                            params.put("reason", reason);
                        }
                        if(silentDateEnd != null) {
                            String silentDateEndStr = CoreDateUtils.formatDate(silentDateEnd, "yyyy年MM月dd日 HH时mm分");
                            params.put("silentDateEnd", silentDateEndStr);
                        }
                    }
                    try {
                        PushSendRecord pushSendRecord = pushComponent.pushSingle(clientDeviceIndex.getVendorClientId(), messageTemplateKey, params, Maps.newHashMap(), clientDeviceIndex.getClientOSType().equals(ClientOSType.IOS) ? PushOsType.IOS : PushOsType.ANDROID);

                        MessageTemplate messageTemplate = messageTemplateApiService.findByTemplateKey(messageTemplateKey);
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
    }

}
