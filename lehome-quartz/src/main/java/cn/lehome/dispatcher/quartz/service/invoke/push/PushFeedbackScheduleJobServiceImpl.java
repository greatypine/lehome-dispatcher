package cn.lehome.dispatcher.quartz.service.invoke.push;

import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service("pushFeedbackScheduleJobService")
public class PushFeedbackScheduleJobServiceImpl extends AbstractInvokeServiceImpl{

    @Autowired
    private EventBusComponent eventBusComponent;

    @Override
    public void doInvoke(Map<String, String> params) {

        eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.PUSH_FEEDBACK_EVENT,0l));

    }

}
