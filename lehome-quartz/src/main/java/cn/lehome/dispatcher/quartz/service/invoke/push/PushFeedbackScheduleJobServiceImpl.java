package cn.lehome.dispatcher.quartz.service.invoke.push;

import cn.lehome.base.api.tool.compoment.jms.EventBusComponent;
import cn.lehome.base.api.tool.constant.EventConstants;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import com.alibaba.fastjson.JSON;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
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
