package cn.lehome.dispatcher.logs.listener;

import cn.lehome.dispatcher.logs.service.HttpRequestSuccessLogSerivce;
import cn.lehome.framework.base.api.core.constant.RequestLogTopicContants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * 成功队列监听
 * Created by zuoguodong on 2018/3/24
 */
@Configuration
@ComponentScan({"cn.lehome.dispatcher.logs.service.impl"})
public class HttpRequestSuccessListener implements HttpRequestBaseListener{

    private final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private HttpRequestSuccessLogSerivce httpRequestSuccessLogService;

    @Override
    @KafkaListener(topics = RequestLogTopicContants.REQIEST_SUCCESS_LOG_TOPIC,containerFactory = "kafkaListenerContainerFactory")
    public void listen(String httpRequestLogJson) {
        try {
            httpRequestSuccessLogService.addLog(httpRequestLogJson);
        }catch(Exception e){
            logger.error("成功队列监听,记录日志信息时出错:",e);
        }
    }

    @Override
    @Bean
    public HttpRequestBaseListener listener() {
        return new HttpRequestSuccessListener();
    }

}
