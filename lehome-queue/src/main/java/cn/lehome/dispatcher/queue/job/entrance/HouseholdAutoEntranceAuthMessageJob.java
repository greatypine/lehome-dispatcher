package cn.lehome.dispatcher.queue.job.entrance;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.entrance.HouseholdAutoEntranceMessageListener;
import cn.lehome.dispatcher.queue.listener.entrance.RefreshEntranceMessageListener;
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
public class HouseholdAutoEntranceAuthMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer householdInfoAutoEntranceAuthMessageJob() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(simpleJmsQueueFactoryBean.getInstance(EventConstants.HOUSEHOLD_ENTRANCE_AUTH_EVENT.getTopicName()));
        defaultMessageListenerContainer.setMessageListener(householdAutoEntranceMessageListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public HouseholdAutoEntranceMessageListener householdAutoEntranceMessageListener() {
        return new HouseholdAutoEntranceMessageListener();
    }
}