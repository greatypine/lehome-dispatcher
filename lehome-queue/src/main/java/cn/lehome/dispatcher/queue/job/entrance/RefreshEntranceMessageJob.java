package cn.lehome.dispatcher.queue.job.entrance;

import cn.lehome.base.api.tool.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.auth.RemoveUserHouseSendMessageListener;
import cn.lehome.dispatcher.queue.listener.entrance.RefreshEntranceMessageListener;
import cn.lehome.framework.base.api.core.compoment.jms.EventBusJmsVirtualTopicQueueFactoryBean;
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
public class RefreshEntranceMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer refreshEntranceMessageListenerContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(simpleJmsQueueFactoryBean.getInstance(EventConstants.REFRESH_ENTRANCE_EVENT.getTopicName()));
        defaultMessageListenerContainer.setMessageListener(refreshEntranceMessageListener());
        defaultMessageListenerContainer.setMaxConcurrentConsumers(10);
        defaultMessageListenerContainer.setConcurrentConsumers(2);
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public RefreshEntranceMessageListener refreshEntranceMessageListener() {
        return new RefreshEntranceMessageListener();
    }
}
