package cn.lehome.dispatcher.quartz.service;

import cn.lehome.base.api.common.service.job.ScheduleJobApiService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

/**
 * Created by wuzhao on 2018/2/4.
 */
public abstract class AbstractInvokeServiceImpl extends AbstractServiceImpl implements InvokeService {

    @Autowired
    protected ScheduleJobApiService scheduleJobApiService;


    @Override
    public void invoke(Long scheduleId, Map<String, String> params) {
        try {
            this.doInvoke(params);
        } catch (Exception e) {
            logger.error("执行定时任务错误:", e);
        } finally {
            if (!scheduleId.equals(0L)) {
                scheduleJobApiService.complete(scheduleId);
            }
        }
    }

   public abstract void doInvoke(Map<String, String> params);
}
