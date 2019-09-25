package cn.lehome.dispatcher.queue.job.car;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.car.CarRosterSyncListener;
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
public class CarRosterSyncMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer carRosterSyncMessageListenerContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(simpleJmsQueueFactoryBean.getInstance(EventConstants.CAR_ROSTER_EVENT.getTopicName()));
        defaultMessageListenerContainer.setMessageListener(carRosterSyncListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public CarRosterSyncListener carRosterSyncListener() {
        return new CarRosterSyncListener();
    }
}
