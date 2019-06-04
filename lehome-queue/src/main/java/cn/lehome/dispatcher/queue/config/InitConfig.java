package cn.lehome.dispatcher.queue.config;

import cn.lehome.base.api.common.component.idgenerator.RedisIdGeneratorComponent;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.component.message.push.PushComponent;
import cn.lehome.base.api.thirdparty.compoment.gigold.GigoldPaymentComponent;
import cn.lehome.framework.base.api.core.compoment.jms.EventBusJmsVirtualTopicQueueFactoryBean;
import cn.lehome.framework.base.api.core.compoment.jms.SimpleJmsQueueFactoryBean;
import cn.lehome.framework.base.api.core.compoment.loader.LoaderServiceComponent;
import cn.lehome.framework.configuration.properties.jms.ActiveMQProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by wuzhao on 2018/3/12.
 */
@Configuration
public class InitConfig {

    @Autowired
    private ActiveMQProperties activeMQProperties;

    @Bean
    public SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean() {
        SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean = new SimpleJmsQueueFactoryBean();
        simpleJmsQueueFactoryBean.setGlobalPrefix(activeMQProperties.getPrefix());
        return simpleJmsQueueFactoryBean;
    }

    @Bean
    public EventBusJmsVirtualTopicQueueFactoryBean eventBusJmsVirtualTopicQueueFactoryBean() {
        EventBusJmsVirtualTopicQueueFactoryBean eventBusJmsVirtualTopicQueueFactoryBean = new EventBusJmsVirtualTopicQueueFactoryBean();
        eventBusJmsVirtualTopicQueueFactoryBean.setGlobalPrefix(activeMQProperties.getPrefix());
        return eventBusJmsVirtualTopicQueueFactoryBean;
    }

    @Bean
    public PushComponent pushComponent() {
        return new PushComponent();
    }

    @Bean
    public EventBusComponent eventBusComponent() {
        return new EventBusComponent();
    }

    @Bean
    public GigoldPaymentComponent gigoldPaymentComponent() {
        return new GigoldPaymentComponent();
    }

    @Bean
    public RedisIdGeneratorComponent redisIdGeneratorComponent() {
        return new RedisIdGeneratorComponent();
    }

    @Bean
    public LoaderServiceComponent loaderServiceComponent() {
        return new LoaderServiceComponent();
    }




}
