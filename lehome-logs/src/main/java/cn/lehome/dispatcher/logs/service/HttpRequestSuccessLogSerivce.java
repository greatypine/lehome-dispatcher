package cn.lehome.dispatcher.logs.service;

import cn.lehome.framework.base.api.core.bean.HttpRequestLogBean;

import java.util.List;

/**
 * 请求日志处理服务接口
 * Created by zuoguodong on 2018/3/24
 */
public interface HttpRequestSuccessLogSerivce {

    /**
     * 添加日志
     * @param httpRequestLog
     */
    void addLog(String httpRequestLog);


}
