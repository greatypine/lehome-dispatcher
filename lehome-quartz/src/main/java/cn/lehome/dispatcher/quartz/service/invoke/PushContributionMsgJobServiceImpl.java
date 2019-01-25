package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.advertising.bean.task.RateSettingInfo;
import cn.lehome.base.api.tool.bean.device.ClientDevice;
import cn.lehome.base.api.tool.bean.device.PushDeviceInfo;
import cn.lehome.base.api.tool.compoment.message.push.PushComponent;
import cn.lehome.base.api.tool.constant.MessageKeyConstants;
import cn.lehome.base.api.tool.service.device.DeviceApiService;
import cn.lehome.base.api.user.bean.asset.UserBeanFlowInfo;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.asset.UserBeanFlowApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.tool.entity.enums.device.PushVendorType;
import cn.lehome.bean.tool.entity.enums.push.PushOsType;
import cn.lehome.dispatcher.quartz.cache.RateSettingCache;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.bean.core.enums.ClientOSType;
import cn.lehome.framework.bean.core.enums.ClientType;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by zuoguodong on 2018/5/25
 */
@Service("pushContributionMsgJobService")
public class PushContributionMsgJobServiceImpl extends AbstractInvokeServiceImpl {

    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Autowired
    private PushComponent pushComponent;

    @Autowired
    private UserBeanFlowApiService userBeanFlowApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private DeviceApiService deviceApiService;

    @Autowired
    private RateSettingCache rateSettingCache;

    @Override
    public void doInvoke(Map<String, String> params) {
        List<UserBeanFlowInfo> userBeanFlowInfoList = userBeanFlowApiService.findContributionBeanCount(getStartDate());
        userBeanFlowInfoList.forEach(userBeanFlowInfo -> {
            UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userBeanFlowInfo.getUserId());
            Map<String, String> param= Maps.newHashMap();
            param.put("amount", String.valueOf(userBeanFlowInfo.getOperationNum()));
            Map<String,String> forwardParams = Maps.newHashMap();
            forwardParams.put("beanNum",String.valueOf(userInfoIndex.getBeanNum()));
            forwardParams.put("depositNum",String.valueOf(userInfoIndex.getDepositNum()));
            RateSettingInfo rateSettingInfo = rateSettingCache.get(0);
            forwardParams.put("amt",String.valueOf(rateSettingInfo.getAmt()));
            forwardParams.put("bean",String.valueOf(rateSettingInfo.getBeanNum()));
            this.pushMessage(userInfoIndex.getClientId(), MessageKeyConstants.CONTRIBUTION_BEAN, param, forwardParams);
            logger.info("向用户：" + userInfoIndex.getPhone() + "推送消息，得了：" + String.valueOf(userBeanFlowInfo.getOperationNum())+"豆");
        });
    }

    private static Date getStartDate(){
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        calendar.set(Calendar.MILLISECOND,0);
        return calendar.getTime();
    }

    /**
     *  推送消息
     * @param clientId clientID
     * @param messageKey message类型
     * @param params 参数
     * @param forwardParams APP传递参数
     */
    private void pushMessage(String clientId, String messageKey, Map<String, String> params, Map<String, String> forwardParams){
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

            pushComponent.pushSingle(pushDeviceInfo.getVendorClientId(), messageKey, params, forwardParams, clientDevice.getClientOSType().equals(ClientOSType.ANDROID) ? PushOsType.ANDROID : PushOsType.IOS);
        } catch (Exception e) {
            logger.error(String.format("推送消息失败! clientId = %s", clientId), e);
        }
    }
}
