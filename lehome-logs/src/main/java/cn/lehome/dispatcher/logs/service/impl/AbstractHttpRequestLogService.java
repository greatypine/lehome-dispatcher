package cn.lehome.dispatcher.logs.service.impl;

import cn.lehome.framework.base.api.core.bean.HttpRequestLogBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by zuoguodong on 2018/3/24
 */
public abstract class AbstractHttpRequestLogService implements Runnable{

    @Value("${properties.logs.cache.size}")
    private int logsCacheSize;

    @Value("${properties.logs.save.period}")
    private int period;

    private final transient Logger logger = LoggerFactory.getLogger(this.getClass());

    private List<HttpRequestLogBean> list = new ArrayList<>(logsCacheSize);

    protected void addList(HttpRequestLogBean httpRequestLogBean){
        synchronized(list) {
            list.add(httpRequestLogBean);
            if (list.size() == logsCacheSize) {
                try {
                    this.saveLog(list);
                }catch(Exception e){
                    logger.error("保存日志出错" + e.getMessage());
                }
                list.clear();
            }
        }
    }

    private void saveList(){
        synchronized(list) {
            if(list.size() == 0){
                return;
            }
            try {
                this.saveLog(list);
            }catch(Exception e){
                logger.error("保存日志出错" + e.getMessage());
            }
            list.clear();
        }
    }

    @PostConstruct
    public void initTimerTask(){
        //产生一个1~8之间的随机数做为第一次执行延时时间，防止任务扎堆执行
        Random r = new Random();
        int initialDelay = r.nextInt(8)+1;
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(this, initialDelay, period, TimeUnit.SECONDS);
    }

    public void run(){
        try {
            this.saveList();
        }catch(Exception e){
            logger.error("定时记录日志时出错",e);
        }
    }

    /**
     * 保存日志
     * @param list
     */
    abstract void saveLog(List<HttpRequestLogBean> list);

}
