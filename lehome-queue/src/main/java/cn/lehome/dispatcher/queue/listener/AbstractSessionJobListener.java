package cn.lehome.dispatcher.queue.listener;

import cn.lehome.base.api.tool.bean.jms.JmsHistory;
import cn.lehome.base.api.tool.service.jms.JmsSendRecordHistoryApiService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.exception.BaseApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.listener.SessionAwareMessageListener;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;

/**
 * Created by wuzhao on 2018/8/24.
 */
public abstract class AbstractSessionJobListener implements SessionAwareMessageListener<Message> {
    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private JmsSendRecordHistoryApiService jmsSendRecordHistoryApiService;

    @Override
    public void onMessage(Message message, Session session) throws JMSException {
        IEventMessage iEventMessage = null;
        try {
            if (message instanceof ObjectMessage) {
                Object object = ((ObjectMessage) message).getObject();
                if (object == null) {
                    logger.error("消息中的对象数据为空");
                    return;
                }
                if (object instanceof IEventMessage) {
                    iEventMessage = (IEventMessage) object;
                } else {
                    logger.error("消息中的对象类型不对");
                    return;
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        String errorMessage = "";
        boolean isSuccess = true;
        try {
            session.commit();
            execute(iEventMessage);
        } catch (BaseApiException e) {
            e.fillCodeDesc();
            logger.error("执行错误", e);
            isSuccess = false;
            errorMessage = e.getCode();
        } catch (Exception e) {
            logger.error("执行错误", e);
            isSuccess = false;
            errorMessage = "DQ9999";
        } finally {
            try {
                JmsHistory jmsHistory = jmsSendRecordHistoryApiService.get(iEventMessage.getJmsMessageId());
                if (jmsHistory == null) {
                    logger.error("消息记录未找到， JmsMessageId = {}", iEventMessage.getJmsMessageId());
                } else {
                    if (isSuccess) {
                        jmsSendRecordHistoryApiService.deal(iEventMessage.getJmsMessageId(), getConsumerId());
                    } else {
                        jmsSendRecordHistoryApiService.dealFailed(iEventMessage.getJmsMessageId(), getConsumerId(), errorMessage);
                    }
                }
            } catch (Exception e) {
                logger.error("处理异常", e);
            }
        }

    }

    public abstract void execute(IEventMessage eventMessage) throws Exception;

    public abstract String getConsumerId();
}
