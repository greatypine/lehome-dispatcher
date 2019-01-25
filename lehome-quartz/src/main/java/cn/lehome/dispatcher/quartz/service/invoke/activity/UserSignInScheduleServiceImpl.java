package cn.lehome.dispatcher.quartz.service.invoke.activity;

import cn.lehome.base.api.advertising.bean.task.QUserSigninInfoBean;
import cn.lehome.base.api.advertising.bean.task.UserSigninInfoBean;
import cn.lehome.base.api.advertising.service.task.UserSigninApiService;
import cn.lehome.base.api.tool.bean.device.ClientDevice;
import cn.lehome.base.api.tool.bean.device.PushDeviceInfo;
import cn.lehome.base.api.tool.bean.message.MessageTemplate;
import cn.lehome.base.api.tool.bean.push.PushSendRecord;
import cn.lehome.base.api.tool.compoment.message.push.PushComponent;
import cn.lehome.base.api.tool.constant.MessageKeyConstants;
import cn.lehome.base.api.tool.service.device.DeviceApiService;
import cn.lehome.base.api.tool.service.message.MessageTemplateApiService;
import cn.lehome.base.api.tool.util.DateUtil;
import cn.lehome.base.api.user.bean.message.UserMessage;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.message.UserMessageApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.tool.entity.enums.device.PushVendorType;
import cn.lehome.bean.tool.entity.enums.push.PushOsType;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.bean.core.enums.ClientOSType;
import cn.lehome.framework.bean.core.enums.ClientType;
import cn.lehome.framework.bean.core.enums.PushGroupType;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 用户签到发送推送提醒
 */
@Service("userSignInScheduleService")
public class UserSignInScheduleServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private UserSigninApiService userSigninApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private DeviceApiService deviceApiService;

    @Autowired
    private PushComponent pushComponent;

    @Autowired
    private MessageTemplateApiService messageTemplateApiService;

    @Autowired
    private UserMessageApiService userMessageApiService;

    private static final int PAGE_SIZE = 30;

    @Override
    public void doInvoke(Map<String, String> params) {
        logger.info("用户签到发送推送提醒任务start");
        Date today = new Date();
        Date yesterday = DateUtil.addDays(DateUtil.getDayBegin(today), -1);

        ApiRequest apiRequest = ApiRequest.newInstance();
        apiRequest.filterGreaterEqual(QUserSigninInfoBean.seriesNum, 3);
        apiRequest.filterLessThan(QUserSigninInfoBean.lastSignInTime, DateUtil.getDayBegin(today));

        int pageIndex = 0;
        ApiResponse<UserSigninInfoBean> apiResponse = userSigninApiService.findAll(apiRequest, ApiRequestPage.newInstance().paging(pageIndex, PAGE_SIZE));
        if (CollectionUtils.isEmpty(apiResponse.getPagedData())) {
            logger.info("无需处理~");
            return;
        }
        int totalPage = (int) Math.floor(apiResponse.getTotal()%PAGE_SIZE == 0 ? apiResponse.getTotal()/PAGE_SIZE : apiResponse.getTotal()/PAGE_SIZE + 1 );
        while (pageIndex < totalPage) {
            apiResponse.getPagedData().stream().parallel().forEach(e -> {
                // 截止到昨天连续签到次数 >=3 && 上次签到时间必须在昨天 否则不符合推送条件
                if (DateUtils.isSameDay(e.getLastSignInTime(), yesterday)) {
                    UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(e.getUserId());
                    if (userInfoIndex != null) {
                        pushMessage(userInfoIndex, MessageKeyConstants.USER_SIGN_IN_PUSH, Maps.newHashMap(), Maps.newHashMap());
                    }
                }

            });
            pageIndex++;
            apiResponse = userSigninApiService.findAll(apiRequest, ApiRequestPage.newInstance().paging(pageIndex, PAGE_SIZE));
        }
        logger.info("用户签到发送推送提醒任务end");
    }

    /**
     *  推送消息
     * @param userInfoIndex clientID
     * @param messageKey message类型
     * @param params 参数
     * @param forwardParams APP传递参数
     */
    private void pushMessage(UserInfoIndex userInfoIndex, String messageKey, Map<String, String> params, Map<String, String> forwardParams){
        String clientId = userInfoIndex.getClientId();
        try {
            ClientDevice clientDevice = deviceApiService.getClientDevice(ClientType.SQBJ, clientId);
            if (clientDevice == null) {
                logger.error("设备信息未找到, clientId = {}", clientId);
                return;
            }
            PushDeviceInfo pushDeviceInfo = deviceApiService.getPushDeviceInfo(clientDevice.getId(), PushVendorType.JIGUANG);
            if (pushDeviceInfo == null) {
                logger.error("推送信息未找到, clientId = {}", clientDevice.getId());
                return;
            }
            PushSendRecord pushSendRecord = pushComponent.pushSingle(pushDeviceInfo.getVendorClientId(), messageKey, params, forwardParams, clientDevice.getClientOSType().equals(ClientOSType.ANDROID) ? PushOsType.ANDROID : PushOsType.IOS);
            MessageTemplate messageTemplate = messageTemplateApiService.findByTemplateKey(messageKey);
            if (messageTemplate.getIsContainsStationLetter().equals(YesNoStatus.YES)){
                UserMessage userMessage = new UserMessage();
                userMessage.setUserId(userInfoIndex.getId());
                userMessage.setPushGroupType(PushGroupType.SYSTEM);
                userMessage.setPushHistoryRecordId(pushSendRecord.getId());
                userMessage.setContent(pushSendRecord.getContent());
                userMessage.setMessageTemplateId(messageTemplate.getId());
                List<UserMessage> userMessageList = Lists.newArrayList();
                userMessageList.add(userMessage);
                userMessageApiService.saveBatch(userMessageList);
            }
        } catch (Exception e) {
            logger.error(String.format("推送消息失败! clientId = %s", clientId), e);
        }
    }

}
