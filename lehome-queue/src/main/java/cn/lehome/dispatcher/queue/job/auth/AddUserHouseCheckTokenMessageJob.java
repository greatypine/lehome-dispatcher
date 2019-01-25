package cn.lehome.dispatcher.queue.job.auth;

import cn.lehome.base.api.tool.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.auth.AddUserHouseCheckTokenListener;
import cn.lehome.dispatcher.queue.listener.auth.RemoveUserHouseCheckTokenListener;
import cn.lehome.framework.base.api.core.compoment.jms.EventBusJmsVirtualTopicQueueFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import javax.jms.ConnectionFactory;

/**
 * Created by jinsheng on 15/11/27.
 */
@Configuration
public class AddUserHouseCheckTokenMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private EventBusJmsVirtualTopicQueueFactoryBean eventBusJmsVirtualTopicQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer addUserHouseCheckTokenMessageListenerContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(eventBusJmsVirtualTopicQueueFactoryBean.getSubscribeInstance(EventConstants.AUTH_HOUSE_EVENT, addUserHouseCheckTokenListener().getConsumerId()));
        defaultMessageListenerContainer.setMessageListener(addUserHouseCheckTokenListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public AddUserHouseCheckTokenListener addUserHouseCheckTokenListener() {
        return new AddUserHouseCheckTokenListener();
    }
}
