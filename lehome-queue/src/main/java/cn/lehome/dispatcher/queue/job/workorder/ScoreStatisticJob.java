package cn.lehome.dispatcher.queue.job.workorder;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.workorder.ScoreStatisticListener;
import cn.lehome.dispatcher.queue.listener.workorder.WorkOrderDispatchVisitUserListener;
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
public class ScoreStatisticJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer scoreStatisticContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(simpleJmsQueueFactoryBean.getInstance(EventConstants.SCORE_STATISTIC_EVENT.getTopicName()));
        defaultMessageListenerContainer.setMessageListener(scoreStatisticListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public ScoreStatisticListener scoreStatisticListener() {
        return new ScoreStatisticListener();
    }
}
