package cn.lehome.dispatcher.quartz.bean;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by wuzhao on 2018/2/4.
 */
public class ScheduleJobParamBean implements Serializable {
    private static final long serialVersionUID = 954274411955490119L;

    private Long id;

    private String invokeServiceName;

    private Map<String, String> params;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getInvokeServiceName() {
        return invokeServiceName;
    }

    public void setInvokeServiceName(String invokeServiceName) {
        this.invokeServiceName = invokeServiceName;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }
}
