package cn.lehome.dispatcher.quartz.service.invoke.statistic;

import cn.lehome.base.api.bigdata.service.execute.StatisticBusinessJobExecuteApiService;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by zuoguodong on 2019/11/26
 */
@Service("statisticBusinessJobService")
public class StatisticBusinessJobServiceImpl extends AbstractInvokeServiceImpl {

    private static final String KEY_STATISTIC_BUSINESS_JOB_ID = "statisticBusinessJobId";

    @Autowired
    private StatisticBusinessJobExecuteApiService statisticBusinessJobExecuteApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        statisticBusinessJobExecuteApiService.executeJob(Long.valueOf(params.get(KEY_STATISTIC_BUSINESS_JOB_ID)));
    }
}
