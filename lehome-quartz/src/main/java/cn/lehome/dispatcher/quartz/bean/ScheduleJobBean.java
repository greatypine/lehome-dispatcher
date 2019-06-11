package cn.lehome.dispatcher.quartz.bean;

import cn.lehome.bean.common.enums.job.SchedulerStatus;
import cn.lehome.framework.bean.core.enums.YesNoStatus;

import java.io.Serializable;

/**
 * Created by wuzhao on 2018/1/31.
 */
public class ScheduleJobBean implements Serializable {
    private static final long serialVersionUID = 2052297046168976010L;

    public static final String JOB_PARAM_KEY    = "jobParam";

    /** 任务id */
    private Long id;

    /** 任务别名 */
    private String aliasName;

    /** 任务名称 */
    private String jobName;

    /** 任务分组 */
    private String jobGroup;

    /** 任务状态 */
    private SchedulerStatus status;

    /** 任务运行时间表达式 */
    private String cronExpression;

    /** 是否异步 */
    private YesNoStatus isSync;

    /** 是否只是执行一次 */
    private YesNoStatus isOnce;

    /** 执行service */
    private String exeServiceName;

    /** 执行参数 */
    private String exeParams;

    private String jobTrigger;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobGroup() {
        return jobGroup;
    }

    public void setJobGroup(String jobGroup) {
        this.jobGroup = jobGroup;
    }

    public SchedulerStatus getStatus() {
        return status;
    }

    public void setStatus(SchedulerStatus status) {
        this.status = status;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public YesNoStatus getIsSync() {
        return isSync;
    }

    public void setIsSync(YesNoStatus isSync) {
        this.isSync = isSync;
    }

    public YesNoStatus getIsOnce() {
        return isOnce;
    }

    public void setIsOnce(YesNoStatus isOnce) {
        this.isOnce = isOnce;
    }

    public String getExeServiceName() {
        return exeServiceName;
    }

    public void setExeServiceName(String exeServiceName) {
        this.exeServiceName = exeServiceName;
    }

    public String getExeParams() {
        return exeParams;
    }

    public void setExeParams(String exeParams) {
        this.exeParams = exeParams;
    }

    public String getJobTrigger() {
        return jobTrigger;
    }

    public void setJobTrigger(String jobTrigger) {
        this.jobTrigger = jobTrigger;
    }
}
