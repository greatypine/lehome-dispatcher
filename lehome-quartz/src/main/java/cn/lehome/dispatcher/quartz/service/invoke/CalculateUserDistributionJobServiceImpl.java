package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.bigdata.service.distribution.CalculateDistributionApiService;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by zuoguodong on 2018/10/15
 */
@Service("calculateUserDistributionJobService")
public class CalculateUserDistributionJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private CalculateDistributionApiService calculateDistributionApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        logger.info("用户分布统计开始");
        calculateDistributionApiService.calculateDistribution();
        logger.info("用户分布统计结束");
    }
}
