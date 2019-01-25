package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.bigdata.service.activity.CalculateKeepActivityApiService;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by zuoguodong on 2018/9/17
 */
@Service("calculateKeepActivityUserJobService")
public class CalculateKeepActivityUserJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private CalculateKeepActivityApiService calculateKeepActivityApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        logger.info("用户留存定时任务开始执行");
        calculateKeepActivityApiService.calculateUserKeep();
        logger.info("用户留存定时任务执行完毕");
    }
}
