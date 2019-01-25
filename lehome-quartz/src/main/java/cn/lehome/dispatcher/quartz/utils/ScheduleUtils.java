package cn.lehome.dispatcher.quartz.utils;

import cn.lehome.dispatcher.quartz.bean.ScheduleJobBean;
import cn.lehome.dispatcher.quartz.bean.ScheduleJobParamBean;
import cn.lehome.dispatcher.quartz.exception.ScheduleException;
import cn.lehome.dispatcher.quartz.job.AsyncJobFactory;
import cn.lehome.dispatcher.quartz.job.SyncJobFactory;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ScheduleUtils {

    /** 日志对象 */
    private static final Logger logger = LoggerFactory.getLogger(ScheduleUtils.class);

    /**
     * 获取触发器key
     * 
     */
    public static TriggerKey getTriggerKey(String jobName, String jobGroup) {

        return TriggerKey.triggerKey(jobName, jobGroup);
    }

    /**
     * 获取表达式触发器
     *
     * @param scheduler the scheduler
     * @param jobName the job name
     * @param jobGroup the job group
     * @return cron trigger
     */
    public static CronTrigger getCronTrigger(Scheduler scheduler, String jobName, String jobGroup) {

        try {
            TriggerKey triggerKey = TriggerKey.triggerKey(jobName, jobGroup);
            return (CronTrigger) scheduler.getTrigger(triggerKey);
        } catch (SchedulerException e) {
            logger.error("获取定时任务CronTrigger出现异常", e);
            throw new ScheduleException("获取定时任务CronTrigger出现异常");
        }
    }

    /**
     * 创建任务
     *
     * @param scheduler the scheduler
     * @param scheduleJobBean the schedule job
     */
    public static void createScheduleJob(Scheduler scheduler, ScheduleJobBean scheduleJobBean) {
        createScheduleJob(scheduler, scheduleJobBean.getJobName(), scheduleJobBean.getJobGroup(),
                scheduleJobBean.getCronExpression(), scheduleJobBean.getIsSync(), scheduleJobBean.getId(), scheduleJobBean.getExeServiceName(), scheduleJobBean.getExeParams());


    }

    /**
     * 创建定时任务
     *
     * @param scheduler the scheduler
     * @param jobName the job name
     * @param jobGroup the job group
     * @param cronExpression the cron expression
     * @param isSync the is sync
     * @param id the id
     * @param invokeServiceName the invokeServiceName
     * @param params the params
     */
    private static void createScheduleJob(Scheduler scheduler, String jobName, String jobGroup,
                                         String cronExpression, YesNoStatus isSync, Long id, String invokeServiceName, String params) {
        try {
            //同步或异步
            Class<? extends Job> jobClass = isSync.equals(YesNoStatus.NO) ? AsyncJobFactory.class : SyncJobFactory.class;

            //构建job信息
            JobDetail jobDetail = JobBuilder.newJob(jobClass).withIdentity(jobName, jobGroup).build();

            //表达式调度构建器
            CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);

            //按新的cronExpression表达式构建一个新的trigger
            CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity(jobName, jobGroup).withSchedule(scheduleBuilder).build();


            ScheduleJobParamBean paramBean = new ScheduleJobParamBean();
            paramBean.setId(id);
            paramBean.setInvokeServiceName(invokeServiceName);
            if (StringUtils.isNotEmpty(params)) {
                paramBean.setParams(JSON.parseObject(params, new TypeReference<Map<String, String>>() {}));
            } else {
                paramBean.setParams(Maps.newHashMap());
            }


            //放入参数，运行时的方法可以获取
            jobDetail.getJobDataMap().put(ScheduleJobBean.JOB_PARAM_KEY, paramBean);

            scheduler.scheduleJob(jobDetail, trigger);
        } catch (Exception e) {
            logger.error("创建定时任务失败", e);
            throw new ScheduleException("创建定时任务失败");
        }
    }

    /**
     * 运行一次任务
     */
    public static void runOnce(Scheduler scheduler, String jobName, String jobGroup) {
        try {
            JobKey jobKey = JobKey.jobKey(jobName, jobGroup);
            scheduler.triggerJob(jobKey);
        } catch (SchedulerException e) {
            logger.error("运行一次定时任务失败", e);
            throw new ScheduleException("创建定时任务失败");
        }
    }

    /**
     * 暂停任务
     */
    public static void pauseJob(Scheduler scheduler, String jobName, String jobGroup) {
        try {
            JobKey jobKey = JobKey.jobKey(jobName, jobGroup);
            scheduler.pauseJob(jobKey);
        } catch (SchedulerException e) {
            logger.error("暂停定时任务失败", e);
            throw new ScheduleException("暂停定时任务失败");
        }
    }

    /**
     * 恢复任务
     */
    public static void resumeJob(Scheduler scheduler, String jobName, String jobGroup) {
        boolean isSuccess = true;
        try {
            JobKey jobKey = JobKey.jobKey(jobName, jobGroup);
            scheduler.resumeJob(jobKey);
        } catch (SchedulerException e) {
            logger.error("恢复定时任务失败", e);
            throw new ScheduleException("恢复定时任务失败");
        }
    }

    /**
     * 获取jobKey
     * @param jobName the job name
     * @param jobGroup the job group
     * @return the job key
     */
    private static JobKey getJobKey(String jobName, String jobGroup) {

        return JobKey.jobKey(jobName, jobGroup);
    }

    /**
     * 更新定时任务
     *
     * @param scheduler the scheduler
     * @param scheduleJobBean the schedule job
     */
    public static void updateScheduleJob(Scheduler scheduler, ScheduleJobBean scheduleJobBean) {
        updateScheduleJob(scheduler, scheduleJobBean.getJobName(), scheduleJobBean.getJobGroup(),
            scheduleJobBean.getCronExpression());
    }

    /**
     * 更新定时任务
     *
     * @param scheduler the scheduler
     * @param jobName the job name
     * @param jobGroup the job group
     * @param cronExpression the cron expression
     */
    private static void updateScheduleJob(Scheduler scheduler, String jobName, String jobGroup,
                                         String cronExpression) {
        try {

            TriggerKey triggerKey = getTriggerKey(jobName, jobGroup);

            //表达式调度构建器
            CronScheduleBuilder scheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);

            CronTrigger trigger = (CronTrigger) scheduler.getTrigger(triggerKey);

            //按新的cronExpression表达式重新构建trigger
            trigger = trigger.getTriggerBuilder().withIdentity(triggerKey).withSchedule(scheduleBuilder).startNow().build();
            Trigger.TriggerState triggerState = scheduler.getTriggerState(trigger.getKey());
            // 忽略状态为PAUSED的任务，解决集群环境中在其他机器设置定时任务为PAUSED状态后，集群环境启动另一台主机时定时任务全被唤醒的bug
            if(!triggerState.name().equalsIgnoreCase("PAUSED")){
                //按新的trigger重新设置job执行
                scheduler.rescheduleJob(triggerKey, trigger);
            }
        } catch (Exception e) {
            logger.error("更新定时任务失败", e);
            throw new ScheduleException("更新定时任务失败");
        }
    }

    /**
     * 删除定时任务
     */
    public static void deleteScheduleJob(Scheduler scheduler, String jobName, String jobGroup) {
        try {
            scheduler.deleteJob(getJobKey(jobName, jobGroup));
        } catch (SchedulerException e) {
            logger.error("删除定时任务失败", e);
            throw new ScheduleException("删除定时任务失败");
        }
    }
}
