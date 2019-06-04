package cn.lehome.dispatcher.queue.job.house;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.house.HouseAddressUpdateListener;
import cn.lehome.dispatcher.queue.listener.house.SyncUserHouseholdMessageListener;
import cn.lehome.framework.base.api.core.compoment.jms.SimpleJmsQueueFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import javax.jms.ConnectionFactory;

/**
 * Created by zuoguodong on 2018/6/4
 */
@Configuration
public class HouseAddressUpdateMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer syncUserHouseholdMessageListenerContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(simpleJmsQueueFactoryBean.getInstance(EventConstants.ADDRESS_NAME_CHANGE_EVENT.getTopicName()));
        defaultMessageListenerContainer.setMessageListener(householdMessageListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public HouseAddressUpdateListener householdMessageListener() {
        return new HouseAddressUpdateListener();
    }
}
