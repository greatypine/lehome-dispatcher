package cn.lehome.dispatcher.queue.listener;

import cn.lehome.base.api.common.bean.jms.JmsHistory;
import cn.lehome.base.api.common.service.jms.JmsSendRecordHistoryApiService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.exception.BaseApiException;
import cn.lehome.framework.bean.core.enums.JmsType;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import javax.jms.Message;
import javax.jms.ObjectMessage;
import java.util.Date;

/**
 * Created by wuzhao on 2018/3/13.
 */
public abstract class AbstractJobListener implements JobListener {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private JmsSendRecordHistoryApiService jmsSendRecordHistoryApiService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static final String JMS_MESSAGE = "JMS_MESSAGE";

    public void onMessage(Message message) {
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
                String key = String.format("%s_%s", JMS_MESSAGE, iEventMessage.getJmsMessageId());
                ValueOperations<String, String> valueOperations = stringRedisTemplate.opsForValue();
                JmsHistory jmsHistory = null;
                if (stringRedisTemplate.hasKey(key)) {
                    jmsHistory = JSON.parseObject(valueOperations.get(key), JmsHistory.class);
                }
                if (jmsHistory == null) {
                    jmsHistory = jmsSendRecordHistoryApiService.get(iEventMessage.getJmsMessageId());
                }

                if (jmsHistory == null) {
                    logger.error("消息记录未找到， JmsMessageId = {}", iEventMessage.getJmsMessageId());
                    return;
                }

                if (!isSuccess && jmsHistory.getJmsType().equals(JmsType.QUEUE)) {
                    jmsHistory.setConsumerId(getConsumerId());
                    jmsHistory.setErrorMessage(errorMessage);
                    jmsHistory.setIsDeal(YesNoStatus.YES);
                    jmsHistory.setIsSuccess("0");
                    jmsHistory.setDealTime(new Date());
                    jmsSendRecordHistoryApiService.save(jmsHistory);
                }

                if (jmsHistory.getJmsType().equals(JmsType.TOPIC)) {
                    boolean isSave = jmsHistory.getIsDeal().equals(YesNoStatus.NO) ? false : true;
                    if (isSave) {
                        if (isSuccess) {
                            jmsSendRecordHistoryApiService.deal(iEventMessage.getJmsMessageId(), getConsumerId());
                        } else {
                            jmsSendRecordHistoryApiService.dealFailed(iEventMessage.getJmsMessageId(), getConsumerId(), errorMessage);
                        }
                    } else {
                        jmsHistory.setIsDeal(YesNoStatus.YES);
                        jmsHistory.setConsumerId(getConsumerId());
                        if (isSuccess) {
                            jmsHistory.setIsSuccess("1");
                        } else {
                            jmsHistory.setIsSuccess("0");
                            jmsHistory.setErrorMessage(errorMessage);
                        }
                        jmsSendRecordHistoryApiService.save(jmsHistory);
                    }
                }
            } catch (Exception e) {
                logger.error("处理异常", e);
            }
        }

    }


}
