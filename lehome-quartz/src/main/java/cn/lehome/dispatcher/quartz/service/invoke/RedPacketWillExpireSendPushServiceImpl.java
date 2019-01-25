package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.activity.bean.advert.Advert;
import cn.lehome.base.api.activity.bean.advert.AdvertRedPacketAllocate;
import cn.lehome.base.api.activity.bean.advert.QAdvertRedPacketAllocate;
import cn.lehome.base.api.activity.service.advert.AdvertApiService;
import cn.lehome.base.api.activity.service.advert.AdvertRedPacketAllocateApiService;
import cn.lehome.base.api.tool.bean.device.ClientDevice;
import cn.lehome.base.api.tool.bean.device.PushDeviceInfo;
import cn.lehome.base.api.tool.compoment.message.push.PushComponent;
import cn.lehome.base.api.tool.constant.MessageKeyConstants;
import cn.lehome.base.api.tool.service.device.DeviceApiService;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.activity.enums.advert.AdvertStatus;
import cn.lehome.bean.tool.entity.enums.device.PushVendorType;
import cn.lehome.bean.tool.entity.enums.push.PushOsType;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.util.StringUtil;
import cn.lehome.framework.bean.core.enums.ClientOSType;
import cn.lehome.framework.bean.core.enums.ClientType;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author yanwenkai
 * @date 2018/7/19
 */
@Service("redPacketWillExpireSendPushService")
public class RedPacketWillExpireSendPushServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private AdvertApiService advertApiServiceNew;

    @Autowired
    private AdvertRedPacketAllocateApiService advertRedPacketAllocateApiServiceNew;

    @Autowired
    private PushComponent pushComponent;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private DeviceApiService deviceApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        String advertId = params.get("advertId");
        logger.info("红包将要过期定时发送推送开始advertId:{}",advertId);
        if (StringUtil.isNotEmpty(advertId)) {
            Advert one = advertApiServiceNew.findOne(Long.valueOf(advertId));
            AdvertStatus status = one.getStatus();
            if (AdvertStatus.PUBLISHED.equals(status)) {
                ApiRequest apiRequest = ApiRequest.newInstance();
                apiRequest.filterEqual(QAdvertRedPacketAllocate.advertId,advertId);
                apiRequest.filterEqual(QAdvertRedPacketAllocate.opened, YesNoStatus.NO);
                apiRequest.filterEqual(QAdvertRedPacketAllocate.drew,YesNoStatus.YES);
                List<AdvertRedPacketAllocate> all = advertRedPacketAllocateApiServiceNew.findAll(apiRequest);
                all.forEach(advertRedPacketAllocate -> {
                    String drewUser = advertRedPacketAllocate.getDrewUser();
                    if (StringUtil.isNotEmpty(drewUser)) {
                        UserInfoIndex byUserId = userInfoIndexApiService.findByUserId(Long.valueOf(drewUser));
                        if (byUserId != null) {
                            this.pushMessage(byUserId.getClientId(), MessageKeyConstants.NOT_DRAW_WILL_EXPIRE, Maps.newHashMap(),Maps.newHashMap());
                        }
                    }
                });
            }
        }
        logger.info("红包将要过期定时发送推送结束。。。。");
    }

    private void pushMessage(String clientId, String messageKey, Map<String, String> params, Map<String, String> forwardParams){
        try {
            ClientDevice clientDevice = deviceApiService.getClientDevice(ClientType.SQBJ, clientId);
            if (clientDevice == null) {
                logger.error("设备信息未找到, clientId = {}", clientId);
                return;
            }
            PushDeviceInfo pushDeviceInfo = deviceApiService.getPushDeviceInfo(clientDevice.getId(), PushVendorType.JIGUANG);
            if (pushDeviceInfo == null) {
                logger.error("推送信息未找到,  clientId = {}", clientDevice.getId());
                return;
            }
            pushComponent.pushSingle(pushDeviceInfo.getVendorClientId(), messageKey, params, forwardParams, clientDevice.getClientOSType().equals(ClientOSType.ANDROID) ? PushOsType.ANDROID : PushOsType.IOS);
        } catch (Exception e) {
            logger.error(String.format("推送消息失败! clientId = %s", clientId), e);
        }
    }
}
