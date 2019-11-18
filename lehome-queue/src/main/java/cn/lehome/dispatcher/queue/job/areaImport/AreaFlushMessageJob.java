package cn.lehome.dispatcher.queue.job.areaImport;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.areaImport.AreaFlushListener;
import cn.lehome.dispatcher.queue.listener.dataImport.ImportDataListener;
import cn.lehome.framework.base.api.core.compoment.jms.SimpleJmsQueueFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import javax.jms.ConnectionFactory;

/**
 * Created by jinsheng on 15/11/27.
 */
@Configuration
public class AreaFlushMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer areaFlushMessageListenerContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(simpleJmsQueueFactoryBean.getInstance(EventConstants.FLUSH_AREA_DATA_EVENT.getTopicName()));
        defaultMessageListenerContainer.setMessageListener(areaFlushListener());
        defaultMessageListenerContainer.setMaxConcurrentConsumers(10);
        defaultMessageListenerContainer.setConcurrentConsumers(2);
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public AreaFlushListener areaFlushListener() {
        return new AreaFlushListener();
    }
}
