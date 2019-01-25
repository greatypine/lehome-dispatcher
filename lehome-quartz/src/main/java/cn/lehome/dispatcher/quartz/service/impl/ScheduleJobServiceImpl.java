package cn.lehome.dispatcher.quartz.service.impl;

import cn.lehome.base.api.tool.bean.job.ScheduleJob;
import cn.lehome.base.api.tool.service.job.ScheduleJobApiService;
import cn.lehome.bean.tool.entity.enums.job.SchedulerStatus;
import cn.lehome.dispatcher.quartz.bean.ScheduleJobBean;
import cn.lehome.dispatcher.quartz.constants.Constants;
import cn.lehome.dispatcher.quartz.exception.ScheduleException;
import cn.lehome.dispatcher.quartz.service.AbstractServiceImpl;
import cn.lehome.dispatcher.quartz.service.ScheduleJobService;
import cn.lehome.dispatcher.quartz.utils.ScheduleUtils;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import com.google.common.collect.Sets;
import org.quartz.CronTrigger;
import org.quartz.Scheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * Created by wuzhao on 2018/2/4.
 */
@Service
public class ScheduleJobServiceImpl extends AbstractServiceImpl implements ScheduleJobService {

    @Autowired
    private Scheduler scheduler;

    @Autowired
    private ScheduleJobApiService scheduleJobApiService;

    private static final String JOB_NAME = "job-check";

    private static final String JOB_GROUP = "lehome";


    @Override
    public void createScheduleJob(ScheduleJob scheduleJob) {
        boolean isSuccess = true;
        try {
            ScheduleJobBean scheduleJobBean = BeanMapping.map(scheduleJob, ScheduleJobBean.class);
            ScheduleUtils.createScheduleJob(scheduler, scheduleJobBean);
        } catch (Exception e) {
            logger.error("创建定时任务错误:", e);
            isSuccess = false;
        } finally {
            if (isSuccess) {
                scheduleJobApiService.updateStatus(scheduleJob.getId(), SchedulerStatus.RUNNING);
            } else {
                scheduleJobApiService.updateStatus(scheduleJob.getId(), SchedulerStatus.CREATE_FAILED);
            }
        }
    }

    @Override
    public void runOnce(ScheduleJob scheduleJob) {
        boolean isSuccess = true;
        try {
            ScheduleUtils.runOnce(scheduler, scheduleJob.getJobName(), scheduleJob.getJobGroup());
        } catch (Exception e) {
            logger.error("创建定时任务错误:", e);
            isSuccess = false;
        } finally {
            if (isSuccess) {
                scheduleJobApiService.updateStatus(scheduleJob.getId(), SchedulerStatus.RUNNING);
            } else {
                scheduleJobApiService.updateStatus(scheduleJob.getId(), SchedulerStatus.START_FAILED);
            }
        }
    }

    @Override
    public void pauseJob(ScheduleJob scheduleJob) {
        boolean isSuccess = true;
        try {
            ScheduleUtils.pauseJob(scheduler, scheduleJob.getJobName(), scheduleJob.getJobGroup());
        } catch (Exception e) {
            logger.error("创建定时任务错误:", e);
            isSuccess = false;
        } finally {
            if (isSuccess) {
                scheduleJobApiService.updateStatus(scheduleJob.getId(), SchedulerStatus.PAUSE);
            } else {
                scheduleJobApiService.updateStatus(scheduleJob.getId(), SchedulerStatus.PAUSE_FAILED);
            }
        }
    }

    @Override
    public void resumeJob(ScheduleJob scheduleJob) {
        boolean isSuccess = true;
        try {
            ScheduleUtils.resumeJob(scheduler, scheduleJob.getJobName(), scheduleJob.getJobGroup());
        } catch (Exception e) {
            logger.error("创建定时任务错误:", e);
            isSuccess = false;
        } finally {
            if (isSuccess) {
                scheduleJobApiService.updateStatus(scheduleJob.getId(), SchedulerStatus.RUNNING);
            } else {
                scheduleJobApiService.updateStatus(scheduleJob.getId(), SchedulerStatus.RESUME_FAILED);
            }
        }
    }

    @Override
    public void updateScheduleJob(ScheduleJob scheduleJob) {
        boolean isSuccess = true;
        try {
            ScheduleJobBean scheduleJobBean = BeanMapping.map(scheduleJob, ScheduleJobBean.class);
            ScheduleUtils.updateScheduleJob(scheduler, scheduleJobBean);
        } catch (Exception e) {
            logger.error("创建定时任务错误:", e);
            isSuccess = false;
        } finally {
            if (isSuccess) {
                scheduleJobApiService.updateStatus(scheduleJob.getId(), SchedulerStatus.RUNNING);
            } else {
                scheduleJobApiService.updateStatus(scheduleJob.getId(), SchedulerStatus.UPDATE_FAILED);
            }
        }
    }

    @Override
    public void deleteScheduleJob(ScheduleJob scheduleJob) {
        boolean isSuccess = true;
        try {
            ScheduleUtils.deleteScheduleJob(scheduler, scheduleJob.getJobName(), scheduleJob.getJobGroup());
        } catch (Exception e) {
            logger.error("创建定时任务错误:", e);
            isSuccess = false;
        } finally {
            if (isSuccess) {
                scheduleJobApiService.delete(scheduleJob.getId());
            } else {
                scheduleJobApiService.updateStatus(scheduleJob.getId(), SchedulerStatus.DELETE_FAILED);
            }
        }
    }

    @Override
    public void initJob() {
        try {
            List<ScheduleJob> scheduleJobList = scheduleJobApiService.findAllByStatus(Sets.newHashSet(SchedulerStatus.RUNNING));
            if (!CollectionUtils.isEmpty(scheduleJobList)) {
                for (ScheduleJob scheduleJob : scheduleJobList) {
                    ScheduleJobBean scheduleJobBean = BeanMapping.map(scheduleJob, ScheduleJobBean.class);
                    CronTrigger cronTrigger = ScheduleUtils.getCronTrigger(scheduler, scheduleJob.getJobName(), scheduleJob.getJobGroup());

                    if (cronTrigger == null) {
                        try {
                            ScheduleUtils.createScheduleJob(scheduler, scheduleJobBean);
                        } catch (Exception e) {
                            logger.error("初始化定时任务错误, id = {}", scheduleJobBean.getId(), e);
                        }
                    } else {
                        try {
                            ScheduleUtils.updateScheduleJob(scheduler, scheduleJobBean);
                        } catch (Exception e) {
                            logger.error("初始化定时任务错误, id = {}", scheduleJobBean.getId(), e);
                        }
                    }
                }
                logger.info("初始化任务完成");
            }

            CronTrigger cronTrigger = ScheduleUtils.getCronTrigger(scheduler, JOB_NAME, JOB_GROUP);
            if (cronTrigger == null) {
                ScheduleUtils.createScheduleJob(scheduler, Constants.CHECK_JOB_BEAN);
            } else {
                ScheduleUtils.updateScheduleJob(scheduler, Constants.CHECK_JOB_BEAN);
            }
            logger.info("启动默认检查任务成功");
        } catch (Exception e) {
            logger.error("初始化定时任务错误", e);
            throw new ScheduleException("初始化定时任务错误");
        }
    }

}
