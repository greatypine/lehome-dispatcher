package cn.lehome.dispatcher.queue.job.login;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.login.NewFirstLoginAuthListener;
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
public class NewFirstLoginAuthMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private EventBusJmsVirtualTopicQueueFactoryBean eventBusJmsVirtualTopicQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer newFirstLoginAuthMessageListenerContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(eventBusJmsVirtualTopicQueueFactoryBean.getSubscribeInstance(EventConstants.NEW_FIRST_LOGIN_EVENT, newFirstLoginAuthListener().getConsumerId()));
        defaultMessageListenerContainer.setMessageListener(newFirstLoginAuthListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public NewFirstLoginAuthListener newFirstLoginAuthListener() {
        return new NewFirstLoginAuthListener();
    }
}
