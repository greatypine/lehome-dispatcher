package cn.lehome.dispatcher.quartz.job;

import cn.lehome.dispatcher.quartz.bean.ScheduleJobBean;
import cn.lehome.dispatcher.quartz.bean.ScheduleJobParamBean;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.quartz.QuartzJobBean;

/**
 * author : fengjing
 * createTime : 2016-08-04
 * description : 异步任务工厂
 * version : 1.0
 */
public class AsyncJobFactory extends QuartzJobBean {

    /* 日志对象 */
    private static final Logger LOG = LoggerFactory.getLogger(AsyncJobFactory.class);
    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LOG.info("AsyncJobFactory execute");
        ScheduleJobParamBean scheduleJobParamBean = (ScheduleJobParamBean) context.getMergedJobDataMap().get(ScheduleJobBean.JOB_PARAM_KEY);
        LOG.info("scheduleId:" + scheduleJobParamBean.getId());
    }
}
