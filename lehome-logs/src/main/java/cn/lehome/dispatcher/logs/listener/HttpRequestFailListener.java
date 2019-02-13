package cn.lehome.dispatcher.logs.listener;

import cn.lehome.dispatcher.logs.service.HttpRequestFailLogSerivce;
import cn.lehome.framework.base.api.core.constant.RequestLogTopicContants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * 失败队列监听
 * Created by zuoguodong on 2018/3/24
 */
@Configuration
public class HttpRequestFailListener implements HttpRequestBaseListener{

    private final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private HttpRequestFailLogSerivce httpRequestFailLogSerivce;

    @Override
    @KafkaListener(topics = RequestLogTopicContants.REQIEST_FAIL_LOG_TOPIC,containerFactory = "kafkaListenerContainerFactory")
    public void listen(String httpRequestLogJson) {
        try {
            httpRequestFailLogSerivce.addLog(httpRequestLogJson);
        }catch(Exception e){
            logger.error("失败队列监听,记录日志信息时出错:",e);
        }
    }

    @Override
    @Bean
    public HttpRequestBaseListener listener() {
        return new HttpRequestFailListener();
    }


}
