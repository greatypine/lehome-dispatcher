package cn.lehome.dispatcher.quartz.compoment;

import cn.lehome.dispatcher.quartz.service.ScheduleJobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ImportResource;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Created by wuzhao on 2018/2/4.
 */
@Component
@ImportResource("classpath:base-api.xml")
public class ScheduleJobInit {
    /** 日志对象 */
    private static final Logger LOG = LoggerFactory.getLogger(ScheduleJobInit.class);

    /** 定时任务service */
    @Autowired
    private ScheduleJobService scheduleJobService;

    @PostConstruct
    public void init() {

        if (LOG.isInfoEnabled()) {
            LOG.info("init");
        }

        scheduleJobService.initJob();

        if (LOG.isInfoEnabled()) {
            LOG.info("end");
        }
    }


}
