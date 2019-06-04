package cn.lehome.dispatcher.queue.service.impl.template;

import cn.lehome.base.api.common.bean.message.MessageTemplate;
import cn.lehome.base.api.common.exception.message.MessageTemplateNotFoundException;
import cn.lehome.base.api.common.service.message.MessageTemplateApiService;
import cn.lehome.dispatcher.queue.service.template.MessageTemplateService;
import cn.lehome.framework.base.api.core.exception.sql.NotFoundRecordException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by zhanghuan on 2018/8/2.
 */
@Service
public class MessageTemplateServiceImpl implements MessageTemplateService{

    @Autowired
    private MessageTemplateApiService messageTemplateApiService;

    @Override
    public MessageTemplate getMessageTemplateIdFromKey(String messageTemplateKey) {
        try {
            MessageTemplate messageTemplate = messageTemplateApiService.findByTemplateKey(messageTemplateKey);
            return messageTemplate;
        } catch (NotFoundRecordException e) {
            throw new MessageTemplateNotFoundException();
        }
    }
}
