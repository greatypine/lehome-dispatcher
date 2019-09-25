package cn.lehome.dispatcher.queue.listener.car;

import cn.lehome.base.api.iot.common.bean.park.CarRosterBean;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import com.alibaba.fastjson.JSON;

/**
 * Created by wuzhao on 2019/5/31.
 */
public class CarRosterSyncListener extends AbstractJobListener {



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof SimpleEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<CarRosterBean> simpleEventMessage = (SimpleEventMessage<CarRosterBean>) eventMessage;
        logger.info("car roster bean : " + JSON.toJSONString(simpleEventMessage.getData()));

    }

    @Override
    public String getConsumerId() {
        return "car_roster_sync";
    }

}
