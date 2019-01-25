package cn.lehome.dispatcher.queue.listener;

import cn.lehome.framework.base.api.core.event.IEventMessage;

import javax.jms.MessageListener;

/**
 * Created by wuzhao on 2018/3/13.
 */
public interface JobListener extends MessageListener {

    void execute(IEventMessage eventMessage) throws Exception;

    String getConsumerId();
}
