package cn.lehome.dispatcher.quartz.constants;

import cn.lehome.dispatcher.quartz.bean.ScheduleJobBean;
import cn.lehome.framework.bean.core.enums.YesNoStatus;

/**
 * Created by wuzhao on 2018/2/4.
 */
public class Constants {

    public static ScheduleJobBean CHECK_JOB_BEAN = new ScheduleJobBean();

    static {
        CHECK_JOB_BEAN.setId(0L);
        CHECK_JOB_BEAN.setCronExpression("0 */1 * * * ?");
        CHECK_JOB_BEAN.setIsSync(YesNoStatus.YES);
        CHECK_JOB_BEAN.setJobName("job-check");
        CHECK_JOB_BEAN.setJobGroup("lehome");
        CHECK_JOB_BEAN.setExeServiceName("checkScheduleJobService");
    }
}
