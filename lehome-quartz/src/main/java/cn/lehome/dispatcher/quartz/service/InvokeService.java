package cn.lehome.dispatcher.quartz.service;

import java.util.Map;

/**
 * Created by wuzhao on 2018/2/4.
 */
public interface InvokeService {

    void invoke(Long scheduleId, Map<String, String> params);
}
