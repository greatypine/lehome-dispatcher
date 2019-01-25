package cn.lehome.dispatcher.quartz.cache;

import cn.lehome.base.api.advertising.bean.task.RateSettingInfo;
import cn.lehome.base.api.advertising.service.task.RateSettingApiService;
import cn.lehome.framework.base.api.core.cache.AbstractLocalCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Created by wuzhao on 2018/2/12.
 */
@Component("rateSettingCache")
public class RateSettingCache extends AbstractLocalCache<Integer, RateSettingInfo> {

    @Autowired
    private RateSettingApiService rateSettingApiService;

    public RateSettingCache() {
        super(5L, TimeUnit.MINUTES);
    }

    @Override
    protected RateSettingInfo getKey(Integer key) {
        return rateSettingApiService.getRateSettingInfo();
    }
}
