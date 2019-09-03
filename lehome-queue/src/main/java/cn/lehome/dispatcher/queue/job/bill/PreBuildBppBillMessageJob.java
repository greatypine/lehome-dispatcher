package cn.lehome.dispatcher.queue.job.bill;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.activity.AdvertStatisticsOfflineListener;
import cn.lehome.dispatcher.queue.listener.bill.PreBuildBppBillListener;
import cn.lehome.framework.base.api.core.compoment.jms.SimpleJmsQueueFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import javax.jms.ConnectionFactory;

/**
 * @author yanwenkai
 * @date 2018/11/7
 */
@Configuration
public class PreBuildBppBillMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer preBuildBppBillContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(simpleJmsQueueFactoryBean.getInstance(EventConstants.PRE_BPP_BILL_EVENT.getTopicName()));
        defaultMessageListenerContainer.setMessageListener(preBuildBppBillListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public PreBuildBppBillListener preBuildBppBillListener() {
        return new PreBuildBppBillListener();
    }
}
