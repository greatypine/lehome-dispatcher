package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by zhuhai on 2018/7/13.
 */
@Service("marketingMessageScheduleJobService")
public class MarketingMessageScheduleJobServiceImpl extends AbstractInvokeServiceImpl{

    @Autowired
    private EventBusComponent eventBusComponent;

    @Override
    public void doInvoke(Map<String, String> params) {
        String pushPlanId = params.get("pushPlanId");
        if (StringUtils.isEmpty(pushPlanId)) {
            logger.error("未获取到发布任务ID");
            return;
        }
        eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.SEND_MARKETING_SMS_EVENT, Long.valueOf(pushPlanId)));
    }
}
