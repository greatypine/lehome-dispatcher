package cn.lehome.dispatcher.queue.job.card;

import cn.lehome.base.api.tool.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.card.CollectCardPrizeSettingListener;
import cn.lehome.framework.base.api.core.compoment.jms.SimpleJmsQueueFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import javax.jms.ConnectionFactory;

/**
 * @author yanwenkai
 * @date 2018/10/18
 */
@Configuration
public class CollectCardPrizeSettingMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer collectCardPrizeSettingListenerContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(simpleJmsQueueFactoryBean.getInstance(EventConstants.COLLECT_CARD_PRIZE_SETTING_MESSAGE_EVENT.getTopicName()));
        defaultMessageListenerContainer.setMessageListener(collectCardPrizeSettingListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public CollectCardPrizeSettingListener collectCardPrizeSettingListener() {
        return new CollectCardPrizeSettingListener();
    }
}
