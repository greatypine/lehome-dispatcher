package cn.lehome.dispatcher.queue.job.login;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.login.FirstLoginAuthListener;
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
public class FirstLoginAuthMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private EventBusJmsVirtualTopicQueueFactoryBean eventBusJmsVirtualTopicQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer firstLoginAuthMessageListenerContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(eventBusJmsVirtualTopicQueueFactoryBean.getSubscribeInstance(EventConstants.FIRST_LOGIN_EVENT, firstLoginAuthListener().getConsumerId()));
        defaultMessageListenerContainer.setMessageListener(firstLoginAuthListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public FirstLoginAuthListener firstLoginAuthListener() {
        return new FirstLoginAuthListener();
    }
}
