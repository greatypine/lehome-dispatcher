package cn.lehome.dispatcher.queue.listener.entrance;

import cn.lehome.base.api.acs.bean.device.Device;
import cn.lehome.base.api.acs.bean.region.Region;
import cn.lehome.base.api.acs.bean.user.User;
import cn.lehome.base.api.acs.service.device.DeviceApiService;
import cn.lehome.base.api.acs.service.region.RegionApiService;
import cn.lehome.base.pro.api.bean.regions.ControlRegions;
import cn.lehome.base.pro.api.bean.regions.QControlRegions;
import cn.lehome.base.pro.api.service.regions.ControlRegionsApiService;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.entrance.AutoEntranceService;
import cn.lehome.framework.base.api.core.compoment.loader.LoaderServiceComponent;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.StringEventMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class BindDeviceMessageListener extends AbstractJobListener {

    @Autowired
    private RegionApiService regionApiService;

    @Autowired
    private ControlRegionsApiService controlRegionsApiService;

    @Autowired
    private LoaderServiceComponent loaderServiceComponent;

    @Autowired
    private AutoEntranceService autoEntranceService;

    @Autowired
    private DeviceApiService acsDeviceApiService;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof StringEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        StringEventMessage stringEventMessage = (StringEventMessage) eventMessage;

        Device device = acsDeviceApiService.findBySn(stringEventMessage.getData());
        if (device == null) {
            logger.error("设备信息未找到, deviceUuid = {}", stringEventMessage.getData());
        }

        Region region = regionApiService.findByDeviceUuid(stringEventMessage.getData());
        if (region == null) {
            logger.error("设备未绑定管控区域, deviceUuid = {}", stringEventMessage.getData());
        }


        ControlRegions controlRegions = controlRegionsApiService.get(Long.valueOf(region.getTraceId()));

        if (controlRegions == null) {
            logger.error("管控区域未找到, id = " + region.getTraceId());
            return;
        }

        if (!region.getAutoAccept()) {
            logger.error("未设置自动授权, 不进行自动授权操作, id = " + region.getId());
            return;
        }

        loaderServiceComponent.load(controlRegions, QControlRegions.positionRelationships);

        List<User> userList = autoEntranceService.loadAllUser(controlRegions);

        if (!CollectionUtils.isEmpty(userList)) {
            regionApiService.batchDeleteUserRegion(region.getId(), userList.stream().map(User::getId).collect(Collectors.toList()));
            regionApiService.batchAddUserRegion(region.getId(), userList.stream().map(User::getId).collect(Collectors.toList()));
        }
    }



    @Override
    public String getConsumerId() {
        return "bind_device";
    }
}
