package cn.lehome.dispatcher.utils.thread;

import cn.lehome.base.api.advertising.bean.task.InviteUserChangeRecord;
import cn.lehome.base.api.advertising.service.task.UserTaskRecordApiService;
import cn.lehome.dispatcher.utils.config.DesUtil;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

public class InviteUserChangeRecordThread implements Runnable {

    private UserTaskRecordApiService userTaskRecordApiService;

    private int pageIndex;

    private int pageSize;

    private String key;

    public InviteUserChangeRecordThread(UserTaskRecordApiService userTaskRecordApiService, int pageIndex, int pageSize, String key) {
        this.userTaskRecordApiService = userTaskRecordApiService;
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
        this.key = key;
    }

    @Override
    public void run() {
        try {
            ApiResponse<InviteUserChangeRecord> apiResponse = userTaskRecordApiService.findAllInviteChange(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(pageIndex, pageSize));
            Collection<InviteUserChangeRecord> pagedData = apiResponse.getPagedData();
            if (pagedData!=null && pagedData.size()>0) {
                List<InviteUserChangeRecord> updList = Lists.newArrayList();

                for (InviteUserChangeRecord inviteUserRecord : pagedData){
                    inviteUserRecord.setInvitePhone(DesUtil.decryptPhone(inviteUserRecord.getInvitePhoneDes(), key).trim());
                    updList.add(inviteUserRecord);
                }
                userTaskRecordApiService.updateInviteChangeBatch(updList);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
