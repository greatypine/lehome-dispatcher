package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.tool.compoment.jms.EventBusComponent;
import cn.lehome.base.api.tool.constant.EventConstants;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by wuzhao on 2018/2/4.
 */
@Service("robotSendPostScheduleJobService")
public class RobotSendPostScheduleJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private EventBusComponent eventBusComponent;


    @Override
    public void doInvoke(Map<String, String> params) {
        String tempPostId = params.get("tempPostId");
        if (StringUtils.isEmpty(tempPostId)) {
            logger.error("为获取到发布任务ID");
            return;
        }
        eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.ROBOT_POST_EVENT, Long.valueOf(tempPostId)));

    }
}
