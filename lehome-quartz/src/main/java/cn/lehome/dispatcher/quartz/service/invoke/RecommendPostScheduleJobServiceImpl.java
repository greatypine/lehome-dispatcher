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
 * Created by zhanghuan on 18/5/14.
 */
@Service("recommendPostScheduleJobService")
public class RecommendPostScheduleJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private EventBusComponent eventBusComponent;

    @Override
    public void doInvoke(Map<String, String> params) {
        String recommendId = params.get("recommendId");
        if (StringUtils.isEmpty(recommendId)) {
            logger.error("为获取到发布任务ID");
            return;
        }
        eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.RECOMMEND_POST_EVENT, Long.valueOf(recommendId)));
    }
}
