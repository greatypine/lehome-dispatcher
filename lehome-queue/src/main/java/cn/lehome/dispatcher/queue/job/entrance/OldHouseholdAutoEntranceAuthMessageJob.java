package cn.lehome.dispatcher.queue.job.entrance;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.entrance.HouseholdAutoEntranceMessageListener;
import cn.lehome.dispatcher.queue.listener.entrance.OldHouseholdAutoEntranceMessageListener;
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
public class OldHouseholdAutoEntranceAuthMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer oldHouseholdInfoAutoEntranceAuthMessageJob() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(simpleJmsQueueFactoryBean.getInstance(EventConstants.OLD_HOUSEHOLD_ENTRANCE_AUTH_EVENT.getTopicName()));
        defaultMessageListenerContainer.setMessageListener(oldHouseholdAutoEntranceMessageListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public OldHouseholdAutoEntranceMessageListener oldHouseholdAutoEntranceMessageListener() {
        return new OldHouseholdAutoEntranceMessageListener();
    }
}
