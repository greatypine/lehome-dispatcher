package cn.lehome.dispatcher.queue.job.task;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.task.UserTaskInitListener;
import cn.lehome.framework.base.api.core.compoment.jms.EventBusJmsVirtualTopicQueueFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import javax.jms.ConnectionFactory;

/**
 * 新手任务数据项补偿消费
 */
@Configuration
public class LoginInitUserTaskMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private EventBusJmsVirtualTopicQueueFactoryBean eventBusJmsVirtualTopicQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer initUserTaskListenerContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(eventBusJmsVirtualTopicQueueFactoryBean.getSubscribeInstance(EventConstants.LOGIN_EVENT, userTaskInitListener().getConsumerId()));
        defaultMessageListenerContainer.setMessageListener(userTaskInitListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public UserTaskInitListener userTaskInitListener() {
        return new UserTaskInitListener();
    }
}
