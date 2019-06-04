package cn.lehome.dispatcher.queue.service.template;


import cn.lehome.base.api.common.bean.message.MessageTemplate;

/**
 * Created by zhanghuan on 2018/8/2.
 */
public interface MessageTemplateService {

    MessageTemplate getMessageTemplateIdFromKey(String messageTemplateKey);
}
