package cn.lehome.dispatcher.queue.job.dataExport;

import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.dispatcher.queue.listener.dataExport.ExportDataListener;
import cn.lehome.framework.base.api.core.compoment.jms.SimpleJmsQueueFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import javax.jms.ConnectionFactory;

/**
 * Created by zuoguodong on 2019/9/26
 */
@Configuration
public class DataExportMessageJob {

    @Autowired
    private ConnectionFactory connectionFactory;

    @Autowired
    private SimpleJmsQueueFactoryBean simpleJmsQueueFactoryBean;

    @Bean
    public DefaultMessageListenerContainer exportDataMessageListenerContainer() {
        DefaultMessageListenerContainer defaultMessageListenerContainer = new DefaultMessageListenerContainer();
        defaultMessageListenerContainer.setConnectionFactory(connectionFactory);
        defaultMessageListenerContainer.setDestination(simpleJmsQueueFactoryBean.getInstance(EventConstants.EXPORT_DATA_EVENT.getTopicName()));
        defaultMessageListenerContainer.setMessageListener(exportDataListener());
        defaultMessageListenerContainer.setSessionTransacted(true);
        return defaultMessageListenerContainer;
    }

    @Bean
    public ExportDataListener exportDataListener() {
        return new ExportDataListener();
    }

}
