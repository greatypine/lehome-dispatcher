package cn.lehome.dispatcher.utils.user;

import cn.lehome.base.api.advertising.bean.task.*;
import cn.lehome.base.api.advertising.service.task.UserTaskRecordApiService;
import cn.lehome.dispatcher.utils.config.DesUtil;
import cn.lehome.dispatcher.utils.thread.InviteUserChangeRecordThread;
import cn.lehome.dispatcher.utils.thread.InviteUserRecordThread;
import cn.lehome.dispatcher.utils.thread.UserDrewStaticsThread;
import cn.lehome.dispatcher.utils.thread.UserTaskRecordThread;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.concurrent.ThreadPoolExecutor;

@Service
public class UserTaskServiceImpl implements UserTaskService {

    @Autowired
    private UserTaskRecordApiService userTaskRecordApiService;

    @Autowired
    private ThreadPoolExecutor userTaskThreadPool;

    private Logger logger = LoggerFactory.getLogger(UserTaskServiceImpl.class);

    @Value("${advertising.sqbj.md5Key}")
    private String KEY;

    private static final int pageSize = 100;

    @Override
    public void refreshUserTaskRecord() {
        ApiRequest apiRequest = ApiRequest.newInstance();
        apiRequest.filterEqual("phone","");
        int pageIndex = 0;
        ApiResponse<UserTaskRecord> apiResponse = userTaskRecordApiService.findAllRecord(apiRequest.filterEqual(QUserTaskRecord.phone, ""), ApiRequestPage.newInstance().paging(pageIndex, pageSize));
        Collection<UserTaskRecord> pagedData = apiResponse.getPagedData();
        while(pagedData.size() > 0) {
            userTaskThreadPool.execute(new UserTaskRecordThread(userTaskRecordApiService,pagedData,KEY));
            apiResponse = userTaskRecordApiService.findAllRecord(apiRequest, ApiRequestPage.newInstance().paging(pageIndex, pageSize));
            pagedData = apiResponse.getPagedData();
            System.out.println("pageNo" + pageIndex);
            pageIndex++;
        }
        while (userTaskThreadPool.getQueue().size() != 0) {
            try {
                System.out.println("refreshUserTaskRecord数据加载完毕，还有" + userTaskThreadPool.getQueue().size() + "任务等执行");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("任务执行完毕");
    }

    @Override
    public void refreshInviteUserRecord() {
        long startTime = System.currentTimeMillis();
        logger.info("start time = {}", startTime);
        int pageIndex = 0;
        ApiResponse<InviteUserRecord> apiResponse = userTaskRecordApiService.findAllInvite(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(pageIndex, pageSize));
        Collection<InviteUserRecord> pagedData = apiResponse.getPagedData();
        if (pagedData != null && pagedData.size() > 0) {
            int totalPage = Long.valueOf(apiResponse.getTotal()).intValue() % pageSize ==0 ? Long.valueOf(apiResponse.getTotal()).intValue() /pageSize : Long.valueOf(apiResponse.getTotal()).intValue() /pageSize +1;
            for (;pageIndex < totalPage ; pageIndex++) {
                userTaskThreadPool.execute(new InviteUserRecordThread(userTaskRecordApiService, pageIndex, pageSize, KEY));
                System.out.println("pageNo" + pageIndex);
            }

        }
        while (userTaskThreadPool.getQueue().size() != 0) {
            try {
                System.out.println("refreshInviteUserRecord数据加载完毕，还有" + userTaskThreadPool.getQueue().size() + "任务等执行");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();
        logger.info("end time = {} 耗时 {} ms", endTime, endTime - startTime);
    }

    @Override
    public void refreshInviteUserChaneRecord() {
        long startTime = System.currentTimeMillis();
        logger.info("start time = {}", startTime);
        int pageIndex = 0;

        ApiResponse<InviteUserChangeRecord> apiResponse = userTaskRecordApiService.findAllInviteChange(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(pageIndex, pageSize));
        Collection<InviteUserChangeRecord> pagedData = apiResponse.getPagedData();
        if (pagedData != null && pagedData.size() > 0) {
            int totalPage = Long.valueOf(apiResponse.getTotal()).intValue() % pageSize ==0 ? Long.valueOf(apiResponse.getTotal()).intValue() /pageSize : Long.valueOf(apiResponse.getTotal()).intValue() /pageSize +1;
            for (;pageIndex < totalPage ; pageIndex++) {
                userTaskThreadPool.execute(new InviteUserChangeRecordThread(userTaskRecordApiService, pageIndex, pageSize, KEY));
                System.out.println("pageNo" + pageIndex);
            }

        }
        while (userTaskThreadPool.getQueue().size() != 0) {
            try {
                System.out.println("refreshInviteUserChaneRecord数据加载完毕，还有" + userTaskThreadPool.getQueue().size() + "任务等执行");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();
        logger.info("end time = {} 耗时 {} ms", endTime , endTime-startTime);
    }

    @Override
    public void refreshDrewStatistcs() {
        long startTime = System.currentTimeMillis();
        logger.info("start time = {}", startTime);
        int pageIndex = 0;

        ApiResponse<UserDrewAmountStatistics> apiResponse = userTaskRecordApiService.findAllDrewStatistics(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(pageIndex, pageSize));
        Collection<UserDrewAmountStatistics> pagedData = apiResponse.getPagedData();
        if (pagedData != null && pagedData.size() > 0) {
            int totalPage = Long.valueOf(apiResponse.getTotal()).intValue() % pageSize ==0 ? Long.valueOf(apiResponse.getTotal()).intValue() /pageSize : Long.valueOf(apiResponse.getTotal()).intValue() /pageSize +1;
            for (;pageIndex < totalPage ; pageIndex++) {
                userTaskThreadPool.execute(new UserDrewStaticsThread(userTaskRecordApiService, pageIndex, pageSize, KEY));
                System.out.println("pageNo" + pageIndex);
            }
            while (userTaskThreadPool.getQueue().size() != 0) {
                try {
                    System.out.println("refreshDrewStatistcs数据加载完毕，还有" + userTaskThreadPool.getQueue().size() + "任务等执行");
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        long endTime = System.currentTimeMillis();
        logger.info("end time = {} 耗时 {} ms", endTime , endTime-startTime);
    }


    public static void main (String[] args) {
        System.out.println(DesUtil.decryptPhone("W2CHalR+aX8DrrfJRYO7uw==", "gigold888@cs").trim());
    }

}
