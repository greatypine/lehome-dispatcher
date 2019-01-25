package cn.lehome.dispatcher.quartz.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PreDestroy;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author zhuwenlong
 * @Description
 * @Product
 * @date 2018/1/30.
 */
@Configuration
public class ThreadPoolConfig {
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolConfig.class);

    ThreadPoolExecutor threadPoolExecutor = null;

    @Bean
    public ThreadPoolExecutor userTaskThreadPool(){
        LinkedBlockingDeque<Runnable> linkedBlockingDeque = new LinkedBlockingDeque<>(1000);
        threadPoolExecutor = new ThreadPoolExecutor(2,
                5,
                30,
                TimeUnit.SECONDS,linkedBlockingDeque,
                new RejectedExecutionHandler(){
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        try {
                            linkedBlockingDeque.put(r);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
        logger.info("线程池初始化完毕");
        return threadPoolExecutor;
    }

    @PreDestroy
    public void destroy(){
        if(threadPoolExecutor != null){
            threadPoolExecutor.shutdown();
        }
        logger.info("线程池销毁完毕");
    }

    public static void main(String args[]){
        ThreadPoolConfig threadPoolConfig = new ThreadPoolConfig();
        ThreadPoolExecutor threadPoolExecutor = threadPoolConfig.userTaskThreadPool();
        for(int i = 0;i<10;i++){
            threadPoolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        System.out.println(".");
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        while(threadPoolExecutor.getActiveCount() != 0){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("over");
    }

}
