package cn.lehome.dispatcher.queue.job.park;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.park.MakeParkCarRegisterListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import cn.lehome.framework.base.api.core.compoment.jms.SimpleJmsQueueFactoryBean;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import javax.jms.ConnectionFactory;
/**
 * @Author: sunwj@sqbj.com
 * @Date: 2019/12/5 3:23 下午
 */
@Configuration
public class MakeParkCarRegisterMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer makeParkCarRegisterListenerContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(simpleJmsQueueFactoryBean.getInstance(EventConstants.PARK_CAR_REGISTER_MESSAGE_EVENT.getTopicName()));
        defaultMessageListenerContainer.setMessageListener(makeParkCarRegisterListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public MakeParkCarRegisterListener makeParkCarRegisterListener() {
        return new MakeParkCarRegisterListener();
    }
}
