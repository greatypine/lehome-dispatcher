package cn.lehome.dispatcher.queue.listener.entrance;

import cn.lehome.base.api.acs.bean.region.Region;
import cn.lehome.base.api.acs.bean.user.User;
import cn.lehome.base.api.acs.service.region.RegionApiService;
import cn.lehome.base.pro.api.bean.regions.ControlRegions;
import cn.lehome.base.pro.api.bean.regions.QControlRegions;
import cn.lehome.base.pro.api.service.regions.ControlRegionsApiService;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.entrance.AutoEntranceService;
import cn.lehome.framework.base.api.core.compoment.loader.LoaderServiceComponent;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class AddControlMessageListener extends AbstractJobListener {

    @Autowired
    private RegionApiService regionApiService;

    @Autowired
    private ControlRegionsApiService controlRegionsApiService;

    @Autowired
    private LoaderServiceComponent loaderServiceComponent;

    @Autowired
    private AutoEntranceService autoEntranceService;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;

        ControlRegions controlRegions = controlRegionsApiService.get(longEventMessage.getData());

        if (controlRegions == null) {
            logger.error("管控区域未找到, id = " + longEventMessage.getData());
            return;
        }

        Region region = regionApiService.findByTraceId(controlRegions.getId().toString());
        if (region == null) {
            logger.error("管控区域未找到, id = " + longEventMessage.getData());
            return;
        }

        if (!region.getAutoAccept()) {
            logger.error("未设置自动授权, 不进行自动授权操作, id = " + longEventMessage.getData());
            return;
        }

        loaderServiceComponent.load(controlRegions, QControlRegions.positionRelationships);

        List<User> userList = autoEntranceService.loadAllUser(controlRegions);

        if (!CollectionUtils.isEmpty(userList)) {
            Set<Long> userSet = userList.stream().map(User::getId).collect(Collectors.toSet());
            regionApiService.batchAddUserRegion(region.getId(), Lists.newArrayList(userSet));
        }
    }



    @Override
    public String getConsumerId() {
        return "add_control";
    }
}
