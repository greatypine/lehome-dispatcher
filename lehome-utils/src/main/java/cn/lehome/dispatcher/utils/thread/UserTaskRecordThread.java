package cn.lehome.dispatcher.utils.thread;

import cn.lehome.base.api.advertising.bean.task.UserTaskRecord;
import cn.lehome.base.api.advertising.service.task.UserTaskRecordApiService;
import cn.lehome.dispatcher.utils.config.DesUtil;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class UserTaskRecordThread implements Runnable {

    private UserTaskRecordApiService userTaskRecordApiService;

    private Collection<UserTaskRecord> pagedData;

    private String key;

    public UserTaskRecordThread(UserTaskRecordApiService userTaskRecordApiService,Collection<UserTaskRecord> pagedData, String key) {
        this.userTaskRecordApiService = userTaskRecordApiService;
        this.pagedData = pagedData;
        this.key = key;
    }

    @Override
    public void run() {
        try {
            if (pagedData!=null && pagedData.size()>0) {
                List<UserTaskRecord> updList = Lists.newArrayList();
                for (UserTaskRecord userTaskRecord : pagedData){
                    String phone = DesUtil.decryptPhone(userTaskRecord.getPhoneDes(), key).trim();
                    userTaskRecord.setPhone(phone);
                    updList.add(userTaskRecord);
                }
                try {
                    userTaskRecordApiService.updateRecordBatch(updList);
                }catch(Exception e){
                    updList.forEach(u->{
                        List<UserTaskRecord> list = new ArrayList<UserTaskRecord>();
                        list.add(u);
                        try {
                            userTaskRecordApiService.updateRecordBatch(list);
                        }catch(Exception e1){
                            System.out.println("id:" + u.getId() + ",phone:" + u.getPhone() + ":" + e1.getMessage());
                        }

                    });
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
