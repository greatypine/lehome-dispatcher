package cn.lehome.dispatcher.utils.thread;

import cn.lehome.base.api.advertising.bean.task.UserDrewAmountStatistics;
import cn.lehome.base.api.advertising.service.task.UserTaskRecordApiService;
import cn.lehome.dispatcher.utils.config.DesUtil;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

public class UserDrewStaticsThread implements Runnable {

    private UserTaskRecordApiService userTaskRecordApiService;

    private int pageIndex;

    private int pageSize;

    private String key;

    public UserDrewStaticsThread(UserTaskRecordApiService userTaskRecordApiService, int pageIndex, int pageSize, String key) {
        this.userTaskRecordApiService = userTaskRecordApiService;
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
        this.key = key;
    }

    @Override
    public void run() {
        try {
            ApiResponse<UserDrewAmountStatistics> apiResponse = userTaskRecordApiService.findAllDrewStatistics(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(pageIndex, pageSize));
            Collection<UserDrewAmountStatistics> pagedData = apiResponse.getPagedData();
            if (pagedData!=null && pagedData.size()>0) {
                List<UserDrewAmountStatistics> updList = Lists.newArrayList();

                for (UserDrewAmountStatistics drewAmountStatistics : apiResponse.getPagedData()){
                    drewAmountStatistics.setPhone(DesUtil.decryptPhone(drewAmountStatistics.getPhoneDes(), key).trim());
                    updList.add(drewAmountStatistics);
                }
                userTaskRecordApiService.updateAllDrewStatisticsBatch(updList);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
