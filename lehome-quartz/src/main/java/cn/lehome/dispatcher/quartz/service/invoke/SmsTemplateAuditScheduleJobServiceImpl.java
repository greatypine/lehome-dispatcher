package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.common.service.message.MessageTemplateApiService;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by wuzhao on 2018/2/4.
 */
@Service("smsTemplateAuditScheduleJobService")
public class SmsTemplateAuditScheduleJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private MessageTemplateApiService messageTemplateApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        messageTemplateApiService.checkAuditRecord();
        logger.info("完成短信模版审核记录检查");
    }


}
