package cn.lehome.dispatcher.logs.listener;

import org.springframework.kafka.support.Acknowledgment;

/**
 * 监听基础接口
 * Created by zuoguodong on 2018/3/24
 */
public interface HttpRequestBaseListener {

    /**
     * 监听方法，所有监听实现此方法
     * @param httpRequestLogJson
     */
    void listen (String httpRequestLogJson);

    /**
     * 注册监听
     * @return
     */
    HttpRequestBaseListener listener();

}
