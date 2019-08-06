package cn.lehome.dispatcher.quartz.service.invoke.workorder;

import cn.lehome.base.api.common.constant.MessageKeyConstants;
import cn.lehome.base.api.common.service.message.MessageSendApiService;
import cn.lehome.base.api.workorder.bean.customercenter.BusinessAcceptOrder;
import cn.lehome.base.api.workorder.bean.customercenter.QBusinessAcceptOrder;
import cn.lehome.base.api.workorder.bean.remind.QTimeoutRemindRecord;
import cn.lehome.base.api.workorder.bean.remind.TimeoutRemindRecord;
import cn.lehome.base.api.workorder.bean.settings.QTimeoutSettings;
import cn.lehome.base.api.workorder.bean.settings.TimeoutSettings;
import cn.lehome.base.api.workorder.service.customercenter.BusinessAcceptOrderApiService;
import cn.lehome.base.api.workorder.service.remind.TimeoutRemindRecordApiService;
import cn.lehome.base.api.workorder.service.settings.TimeoutSettingsApiService;
import cn.lehome.bean.workorder.enums.EnabledStatus;
import cn.lehome.bean.workorder.enums.customercenter.BusinessAcceptStatus;
import cn.lehome.bean.workorder.enums.customercenter.OperationUserType;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.compoment.request.ApiPageRequestHelper;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by zuoguodong on 2018/9/17
 */
@Service("workOrderRemindJobService")
public class WorkOrderRemindJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private BusinessAcceptOrderApiService businessAcceptOrderApiService;

    @Autowired
    private TimeoutSettingsApiService timeoutSettingsApiService;

    @Autowired
    private TimeoutRemindRecordApiService timeoutRemindRecordApiService;

    @Autowired
    private MessageSendApiService messageSendApiService;

    private static DateFormat dateFormat = new SimpleDateFormat("MM-dd HH:mm");

    @Override
    public void doInvoke(Map<String, String> params) {
        logger.info("工单超时提醒任务开始执行");
        List<TimeoutSettings> timeoutSettingsList = timeoutSettingsApiService.findAll(ApiRequest.newInstance().filterEqual(QTimeoutSettings.enabledStatus, EnabledStatus.Enabled));
        Date now = new Date();
        for (TimeoutSettings timeoutSettings : timeoutSettingsList) {
            ApiRequest apiRequest = convertRequest(timeoutSettings, now);
            ApiRequestPage requestPage = ApiRequestPage.newInstance().paging(0, 50);
            List<BusinessAcceptOrder> businessAcceptOrderList = ApiPageRequestHelper.request(apiRequest, requestPage, businessAcceptOrderApiService::findAll);
            for (BusinessAcceptOrder businessAcceptOrder : businessAcceptOrderList) {
                List<TimeoutRemindRecord> timeoutRemindRecords = timeoutRemindRecordApiService.findAll(ApiRequest.newInstance().filterEqual(QTimeoutRemindRecord.businessOrderId, businessAcceptOrder.getId()));
                if (!CollectionUtils.isEmpty(timeoutRemindRecords)) {
                    continue;
                }
                Map<String, String> messageParams = Maps.newHashMap();
                String time = "";
                try {
                    time = dateFormat.format(businessAcceptOrder.getCreateTime());
                } catch (Exception e) {
                    logger.error("时间转换失败");
                }
                messageParams.put("time", time);
                if (businessAcceptOrder.getCreateUserType().equals(OperationUserType.Household)) {
                    messageParams.put("userType", "业主");
                } else {
                    messageParams.put("userType", "物业人员");
                }
                messageParams.put("username", businessAcceptOrder.getCustomerName());
                messageParams.put("position", businessAcceptOrder.getAddress());
                String timeout = timeoutSettings.getNotifyTime() + timeoutSettings.getNotifyTimeUnit().desc();
                messageParams.put("timeout", timeout);
                String status = "";
                switch (timeoutSettings.getConditions()) {
                    case NOTASSIGNED:
                        status = "未派单";
                        break;
                    case ASSIGNED:
                        status = "未处理";
                        break;
                    default:
                        break;
                }
                messageParams.put("status", status);
                try {
                    messageSendApiService.sendSingleSms(timeoutSettings.getNotifyUserPhone(), MessageKeyConstants.WORKORDER_TIMEOUT_REMIND, messageParams);
                    TimeoutRemindRecord timeoutRemindRecord = new TimeoutRemindRecord();
                    timeoutRemindRecord.setBusinessOrderId(businessAcceptOrder.getId().longValue());
                    timeoutRemindRecord.setConditions(businessAcceptOrder.getAcceptStatus());
                    timeoutRemindRecord.setLevelId(businessAcceptOrder.getLevelId());
                    timeoutRemindRecord.setNotifyOpenId(timeoutSettings.getNotifyOpenId());
                    timeoutRemindRecord.setNotifyTime(timeoutSettings.getNotifyTime());
                    timeoutRemindRecord.setNotifyTimeUnit(timeoutSettings.getNotifyTimeUnit());
                    timeoutRemindRecord.setNotifyType(2);
                    timeoutRemindRecord.setNotifyUserPhone(timeoutSettings.getNotifyUserPhone());
                    timeoutRemindRecord.setTimeoutId(timeoutSettings.getId());
                    timeoutRemindRecordApiService.create(timeoutRemindRecord);
                } catch (Exception e) {
                    logger.error("发送短信失败:", e);
                }
            }
        }
        logger.info("工单超时提醒任务执行完毕");
    }

    private ApiRequest convertRequest(TimeoutSettings timeoutSettings, Date now) {
        ApiRequest apiRequest = ApiRequest.newInstance().filterEqual(QBusinessAcceptOrder.areaId, timeoutSettings.getAreaId()).filterEqual(QBusinessAcceptOrder.levelId, timeoutSettings.getLevelId());
        Date time = now;
        switch (timeoutSettings.getNotifyTimeUnit()) {
            case Minute:
                time = DateUtils.addMinutes(now, 0 - timeoutSettings.getNotifyTime());
                break;
            case Hour:
                time = DateUtils.addHours(now, 0 - timeoutSettings.getNotifyTime());
                break;
            case Day:
                time = DateUtils.addDays(now, 0 - timeoutSettings.getNotifyTime());
                break;
            case Month:
                time = DateUtils.addMonths(now, 0 - timeoutSettings.getNotifyTime());
                break;
            default:
                time = DateUtils.addYears(now, 0 - timeoutSettings.getNotifyTime());
                break;
        }
        switch (timeoutSettings.getConditions()) {
            case NOTASSIGNED :
                apiRequest.filterEqual(QBusinessAcceptOrder.acceptStatus, BusinessAcceptStatus.Created);
                apiRequest.filterLessThan(QBusinessAcceptOrder.createTime, time);
                break;
            case ASSIGNED :
                apiRequest.filterEqual(QBusinessAcceptOrder.acceptStatus, BusinessAcceptStatus.Sent);
                apiRequest.filterLessEqual(QBusinessAcceptOrder.sendTime, time);
                break;
            default:
                break;
        }
        return apiRequest;
    }
}
