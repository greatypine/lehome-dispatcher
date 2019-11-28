package cn.lehome.dispatcher.queue.listener.event;

import cn.lehome.base.api.workorder.bean.event.Event;
import cn.lehome.base.api.workorder.service.event.EventApiService;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class EventMessageMessageListener extends AbstractJobListener {

    @Autowired
    private EventApiService eventApiService;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<Event> simpleEventMessage = (SimpleEventMessage) eventMessage;

        Event event = simpleEventMessage.getData();
        // TODO: 2019/11/27  告警按照预案来处理
        eventApiService.create(event);
    }



    @Override
    public String getConsumerId() {
        return "event_message";
    }
}
