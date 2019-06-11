package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.common.bean.job.ScheduleJob;
import cn.lehome.bean.common.enums.job.SchedulerStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.dispatcher.quartz.service.ScheduleJobService;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Created by wuzhao on 2018/2/4.
 */
@Service("checkScheduleJobService")
public class CheckScheduleJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private ScheduleJobService scheduleJobService;


    @Override
    public void doInvoke(Map<String, String> params) {
        checkCreate();
        logger.info("创建任务检查完成");
        checkUpdate();
        logger.info("更新任务检查完成");
        checkStart();
        logger.info("启动任务检查完成");
        checkPause();
        logger.info("暂停任务检查完成");
        checkResume();
        logger.info("恢复任务检查完成");
        checkDelete();
        logger.info("删除任务检查完成");
    }

    private void checkCreate() {
        List<ScheduleJob> scheduleJobList = scheduleJobApiService.findAllByStatus(Sets.newHashSet(SchedulerStatus.CREATING));
        scheduleJobList.forEach(scheduleJob -> scheduleJobService.createScheduleJob(scheduleJob));
    }

    private void checkUpdate() {
        List<ScheduleJob> scheduleJobList = scheduleJobApiService.findAllByStatus(Sets.newHashSet(SchedulerStatus.UPDATING_CRON));
        scheduleJobList.forEach(scheduleJob -> scheduleJobService.updateScheduleJob(scheduleJob));
    }

    private void checkStart() {
        List<ScheduleJob> scheduleJobList = scheduleJobApiService.findAllByStatus(Sets.newHashSet(SchedulerStatus.PREPARE_START));
        scheduleJobList.forEach(scheduleJob -> scheduleJobService.runOnce(scheduleJob));
    }

    private void checkPause() {
        List<ScheduleJob> scheduleJobList = scheduleJobApiService.findAllByStatus(Sets.newHashSet(SchedulerStatus.PAUSING));
        scheduleJobList.forEach(scheduleJob -> scheduleJobService.pauseJob(scheduleJob));
    }

    private void checkResume() {
        List<ScheduleJob> scheduleJobList = scheduleJobApiService.findAllByStatus(Sets.newHashSet(SchedulerStatus.RESUMING));
        scheduleJobList.forEach(scheduleJob -> scheduleJobService.resumeJob(scheduleJob));
    }

    private void checkDelete() {
        List<ScheduleJob> scheduleJobList = scheduleJobApiService.findAllByStatus(Sets.newHashSet(SchedulerStatus.DELETING));
        scheduleJobList.forEach(scheduleJob -> scheduleJobService.deleteScheduleJob(scheduleJob));
    }
}
