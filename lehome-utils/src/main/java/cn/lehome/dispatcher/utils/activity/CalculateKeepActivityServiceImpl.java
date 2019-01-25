package cn.lehome.dispatcher.utils.activity;

import cn.lehome.base.api.bigdata.service.activity.CalculateKeepActivityApiService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by zuoguodong on 2018/9/17
 */
@Service
public class CalculateKeepActivityServiceImpl implements CalculateKeepActivityService {

    @Autowired
    private CalculateKeepActivityApiService calculateKeepActivityApiService;

    @Override
    public void calculate() {
        System.out.println("用户留存开始统计");
        calculateKeepActivityApiService.calculateUserKeep();
        System.out.println("用户留存统计结束");
    }
}
