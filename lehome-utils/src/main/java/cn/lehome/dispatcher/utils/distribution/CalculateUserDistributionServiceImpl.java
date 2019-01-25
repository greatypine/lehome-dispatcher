package cn.lehome.dispatcher.utils.distribution;

import cn.lehome.base.api.bigdata.service.distribution.CalculateDistributionApiService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by zuoguodong on 2018/10/15
 */
@Service
public class CalculateUserDistributionServiceImpl implements CalculateUserDistributionService{

    @Autowired
    private CalculateDistributionApiService calculateDistributionApiService;

    @Override
    public void calculate() {
        System.out.println("用户分布开始统计");
        calculateDistributionApiService.calculateDistribution();
        System.out.println("用户分布统计完毕");
    }
}
