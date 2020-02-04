package cn.lehome.dispatcher.quartz.service.invoke.patroltask;

import cn.lehome.base.api.facility.bean.patrol.tasks.PatrolTasks;
import cn.lehome.base.api.facility.bean.patrol.tasks.QPatrolTasks;
import cn.lehome.base.api.facility.service.patrol.task.PatrolTasksApiService;
import cn.lehome.bean.facility.enums.patrol.task.PatrolTaskStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by zuoguodong on 2020/2/4
 */
@Service("patrolTaskExpiredJobService")
public class PatrolTaskExpiredJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    PatrolTasksApiService patrolTasksApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        logger.info("[巡检任务自动过期]开始执行巡检任务自动过期任务");

        ApiRequest request = ApiRequest.newInstance();

        List<PatrolTaskStatus> statusList = Lists.newArrayList(PatrolTaskStatus.NOT_PATROL, PatrolTaskStatus.PATROLLING);

        // 获取所有结束时间在当前时间之前的任务
        request.filterIn(QPatrolTasks.taskStatus, statusList);
        request.filterLessThan(QPatrolTasks.endTime, getDayBeforeOneDayTime());

        List<PatrolTasks> taskList = patrolTasksApiService.findAll(request);

        for (PatrolTasks tasks : taskList){
            tasks.setTaskStatus(PatrolTaskStatus.EXPIRED);
            patrolTasksApiService.update(tasks);
        }

    }

    private Date getDayBeforeOneDayTime() {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date=new Date();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 999);
        date = calendar.getTime();
        String time = sdf.format(date);
        logger.info("系统前一天23:59:59时间:"+time);
        return date;
    }
}
