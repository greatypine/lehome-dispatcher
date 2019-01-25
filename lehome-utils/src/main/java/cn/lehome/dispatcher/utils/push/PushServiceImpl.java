package cn.lehome.dispatcher.utils.push;

import cn.lehome.base.api.tool.bean.job.ScheduleJob;
import cn.lehome.base.api.tool.service.job.ScheduleJobApiService;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service("pushService")
public class PushServiceImpl implements PushService{

    @Autowired
    private ScheduleJobApiService scheduleJobApiService;

    @Override
    public void createScheduleJob() {

            String year = "*";
            String month = "*";
            String day = "*";
            String hour = "*";
            String minutes = "0/10";
            String second = "0";
            String cronExpression = String.format("%s %s %s %s %s %s %s-%s", second, minutes, hour, day, month, "?", year, year);
            ScheduleJob scheduleJob = new ScheduleJob();
            scheduleJob.setIsSync(YesNoStatus.YES);
            scheduleJob.setIsOnce(YesNoStatus.NO);
            scheduleJob.setJobGroup("push-feedback");
            scheduleJob.setJobName("push-feedback-schedule-job");
            scheduleJob.setAliasName("推送");
            scheduleJob.setCronExpression(cronExpression);
            scheduleJob.setDescription("推送回馈");
            Map<String,String> map = new HashMap<>();
            scheduleJob.setExeParams(JSON.toJSONString(map));
            scheduleJob.setExeServiceName("pushFeedbackScheduleJobService");
            ScheduleJob db = scheduleJobApiService.create(scheduleJob);

    }
}
