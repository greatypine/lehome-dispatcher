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
 * Created by zhuhai on 2018/5/23.
 */
@Service("silentCancelScheduleJobService")
public class SilentCancelScheduleJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private EventBusComponent eventBusComponent;


    @Override
    public void doInvoke(Map<String, String> params) {
        String userId = params.get("userId");
        if (StringUtils.isEmpty(userId)) {
            logger.error("未获取到userID");
            return;
        }
        eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.SILENT_CANCEL_EVENT, Long.valueOf(userId)));

    }
}
