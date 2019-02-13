package cn.lehome.dispatcher.logs.service.impl;

import cn.lehome.base.api.logs.service.LogsApiService;
import cn.lehome.dispatcher.logs.service.HttpRequestFailLogSerivce;
import cn.lehome.framework.base.api.core.bean.HttpRequestLogBean;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by zuoguodong on 2018/3/24
 */
@Service
public class HttpRequestFailLogServiceImpl extends AbstractHttpRequestLogService implements HttpRequestFailLogSerivce {


    @Autowired
    private LogsApiService logsApiService;

    @Override
    public void addLog(String httpRequestLog) {
        HttpRequestLogBean httpRequestLogBean = JSON.parseObject(httpRequestLog,HttpRequestLogBean.class);
        this.addList(httpRequestLogBean);
    }

    @Override
    public void saveLog(List<HttpRequestLogBean> list) {
        logsApiService.saveFailLogs(list);
    }
}
