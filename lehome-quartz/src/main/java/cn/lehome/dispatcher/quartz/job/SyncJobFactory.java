package cn.lehome.dispatcher.quartz.job;

import cn.lehome.dispatcher.quartz.bean.ScheduleJobBean;
import cn.lehome.dispatcher.quartz.bean.ScheduleJobParamBean;
import cn.lehome.dispatcher.quartz.service.InvokeService;
import cn.lehome.framework.base.api.core.compoment.context.SpringContextHolder;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.PersistJobDataAfterExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.quartz.QuartzJobBean;

/**
 * author : fengjing
 * createTime : 2016-08-04
 * description : 同步任务工厂
 * version : 1.0
 */
@PersistJobDataAfterExecution
@DisallowConcurrentExecution
public class SyncJobFactory extends QuartzJobBean {

    /* 日志对象 */
    private static final Logger LOG = LoggerFactory.getLogger(SyncJobFactory.class);

    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        LOG.info("SyncJobFactory execute");
        ScheduleJobParamBean scheduleJobParamBean = (ScheduleJobParamBean) context.getMergedJobDataMap().get(ScheduleJobBean.JOB_PARAM_KEY);
        LOG.info("scheduleId:" + scheduleJobParamBean.getId());
        InvokeService invokeService = null;
        try {
            invokeService = SpringContextHolder.getBean(scheduleJobParamBean.getInvokeServiceName(), InvokeService.class);
        } catch (Exception e) {
            LOG.error("获取serviceBean错误:", e);
        }
        if (invokeService != null) {
            invokeService.invoke(scheduleJobParamBean.getId(), scheduleJobParamBean.getParams());
            LOG.info("调用invokeService完成");
        }
    }
}
