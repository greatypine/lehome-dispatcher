package cn.lehome.dispatcher.queue.listener.house;

import cn.lehome.base.pro.api.event.AddressNameChangeEvent;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.house.AddressChangeService;
import cn.lehome.dispatcher.queue.service.house.AddressChangeServiceHandler;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by wuzhao on 2019/5/31.
 */
public class HouseAddressUpdateListener extends AbstractJobListener {

    @Autowired
    private AddressChangeServiceHandler addressChangeServiceHandler;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof SimpleEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<AddressNameChangeEvent> simpleEventMessage = (SimpleEventMessage) eventMessage;
        AddressNameChangeEvent addressNameChangeEvent = simpleEventMessage.getData();
        AddressChangeService addressChangeService = addressChangeServiceHandler.getAddressChangeService(addressNameChangeEvent.getExtendType());
        addressChangeService.changeName(addressNameChangeEvent.getId());
    }

    @Override
    public String getConsumerId() {
        return "house_address_update";
    }
}
