package cn.lehome.dispatcher.quartz.service;

import cn.lehome.base.api.tool.bean.job.ScheduleJob;

/**
 * Created by wuzhao on 2018/2/4.
 */
public interface ScheduleJobService {

    void createScheduleJob(ScheduleJob scheduleJob);

    void runOnce(ScheduleJob scheduleJob);

    void pauseJob(ScheduleJob scheduleJob);

    void resumeJob(ScheduleJob scheduleJob);

    void updateScheduleJob(ScheduleJob scheduleJob);

    void deleteScheduleJob(ScheduleJob scheduleJob);

    void initJob();
}
