package cn.lehome.dispatcher.logs.service.impl;

import cn.lehome.base.api.logs.service.LogsApiService;
import cn.lehome.dispatcher.logs.service.HttpRequestSuccessLogSerivce;
import cn.lehome.framework.base.api.core.bean.HttpRequestLogBean;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by zuoguodong on 2018/3/24
 */
@Service
public class HttpRequestSuccessLogServiceImpl extends AbstractHttpRequestLogService implements HttpRequestSuccessLogSerivce {

    @Autowired
    private LogsApiService logsApiService;

    @Override
    public void addLog(String httpRequestLog) {
        HttpRequestLogBean httpRequestLogBean = JSON.parseObject(httpRequestLog,HttpRequestLogBean.class);
        this.addList(httpRequestLogBean);
    }


    public void saveLog(List<HttpRequestLogBean> list) {
        logsApiService.saveSuccessLogs(list);
    }


}
