package cn.lehome.dispatcher.utils.thread;

import cn.lehome.base.api.advertising.bean.task.InviteUserRecord;
import cn.lehome.base.api.advertising.service.task.UserTaskRecordApiService;
import cn.lehome.dispatcher.utils.config.DesUtil;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;

public class InviteUserRecordThread implements Runnable {

    private UserTaskRecordApiService userTaskRecordApiService;


    private int pageIndex;

    private int pageSize;

    private String key;

    public InviteUserRecordThread(UserTaskRecordApiService userTaskRecordApiService, int pageIndex, int pageSize, String key) {
        this.userTaskRecordApiService = userTaskRecordApiService;
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
        this.key = key;
    }

    @Override
    public void run() {
        try {
            ApiResponse<InviteUserRecord> apiResponse = userTaskRecordApiService.findAllInvite(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(pageIndex, pageSize));
            Collection<InviteUserRecord> pagedData = apiResponse.getPagedData();
            if (pagedData!=null && pagedData.size()>0) {
                ArrayList<InviteUserRecord> updList = Lists.newArrayList();

                for (InviteUserRecord inviteUserRecord : apiResponse.getPagedData()){
                    inviteUserRecord.setPhone(DesUtil.decryptPhone(inviteUserRecord.getPhoneDes(), key).trim());
                    inviteUserRecord.setBeInvitePhone(DesUtil.decryptPhone(inviteUserRecord.getBeInvitePhoneDes(), key).trim());
                    updList.add(inviteUserRecord);
                }
                userTaskRecordApiService.updateInviteBatch(updList);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
