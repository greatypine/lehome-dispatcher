package cn.lehome.dispatcher.quartz.service.invoke.wechat;

import cn.lehome.base.api.advertising.bean.task.UserTaskOperationRecord;
import cn.lehome.base.api.advertising.bean.wechat.QWeChatTaskOperationRecord;
import cn.lehome.base.api.advertising.bean.wechat.WeChatTaskOperationRecord;
import cn.lehome.base.api.advertising.service.task.UserTaskOperationRecordApiService;
import cn.lehome.base.api.advertising.service.wechat.WeChatTaskOperationRecordApiService;
import cn.lehome.base.api.operation.bean.mkt.QShareRecord;
import cn.lehome.base.api.operation.bean.mkt.ShareRecord;
import cn.lehome.base.api.operation.service.mkt.ShareRecordApiService;
import cn.lehome.base.api.tool.bean.wechat.QWechatSyncTask;
import cn.lehome.base.api.tool.bean.wechat.WechatSyncTask;
import cn.lehome.base.api.tool.service.wechat.WechatSyncTaskApiService;
import cn.lehome.base.api.user.bean.asset.QUserBeanFlowInfo;
import cn.lehome.base.api.user.bean.asset.UserBeanFlowInfo;
import cn.lehome.base.api.user.service.asset.UserBeanFlowApiService;
import cn.lehome.base.api.user.service.wechat.WeChatBeanFlowApiService;
import cn.lehome.bean.tool.entity.enums.wechat.SyncStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.bean.core.enums.Operation;
import cn.lehome.framework.bean.core.enums.UserIdType;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service("wechatSyncScheduleJobService")
public class WechatSyncScheduleJobServiceImpl extends AbstractInvokeServiceImpl{

    @Autowired
    private WechatSyncTaskApiService wechatSyncTaskApiService;

    @Autowired
    private WeChatBeanFlowApiService weChatBeanFlowApiService;

    @Autowired
    private UserBeanFlowApiService userBeanFlowApiService;

    @Autowired
    private WeChatTaskOperationRecordApiService weChatTaskOperationRecordApiService;

    @Autowired
    private UserTaskOperationRecordApiService userTaskOperationRecordApiService;

    @Autowired
    private ShareRecordApiService shareRecordApiService;

    private static final Integer PAGE_SIZE = 20;

    @Override
    public void doInvoke(Map<String, String> params) {
        logger.info("同步微信小程序用户流水记录start");
        ApiRequest apiRequest = ApiRequest.newInstance();
        apiRequest.filterIn(QWechatSyncTask.status, Lists.newArrayList(SyncStatus.INIT, SyncStatus.EXEC_FAIL));
        apiRequest.filterLessEqual(QWechatSyncTask.retryNum, 3);

        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(0, PAGE_SIZE);
        ApiResponse<WechatSyncTask> apiResponse = wechatSyncTaskApiService.findAll(apiRequest, apiRequestPage);
        if (CollectionUtils.isEmpty(apiResponse.getPagedData())) {
            logger.info("暂无数据处理");
            return;
        }
        while (true) {
            if (CollectionUtils.isEmpty(apiResponse.getPagedData())) {
                break;
            }
            apiResponse.getPagedData().forEach(e -> {
                try {
                    e = wechatSyncTaskApiService.updateTaskStatus(e.getId(), SyncStatus.EXEC_ING);
                    if (exec(e)) {
                        wechatSyncTaskApiService.updateTaskStatus(e.getId(), SyncStatus.SUCCESS);
                    } else {
                        wechatSyncTaskApiService.updateTaskStatus(e.getId(), SyncStatus.EXEC_FAIL);
                    }
                } catch (Exception e1) {
                    logger.error("同步用户流水错误！userId=" + e.getUserId(), e1);
                    wechatSyncTaskApiService.updateTaskStatus(e.getId(), SyncStatus.EXEC_FAIL);
                }
            });
            apiResponse = wechatSyncTaskApiService.findAll(apiRequest, apiRequestPage);
        }
        logger.info("同步微信小程序用户流水记录end");
    }

    private boolean exec(WechatSyncTask wechatSyncTask) {
        boolean ifAssetSyncSuccess = true;
        if (YesNoStatus.NO.equals(wechatSyncTask.getIfAssetSync())) {
            ApiRequest apiRequest = ApiRequest.newInstance();
            apiRequest.filterEqual(QUserBeanFlowInfo.isSync, YesNoStatus.NO);
            apiRequest.filterEqual(QUserBeanFlowInfo.userId, wechatSyncTask.getRelationId());

            ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
            apiRequestPage.paging(0, PAGE_SIZE);
            ApiResponse<UserBeanFlowInfo> apiResponse = weChatBeanFlowApiService.findAll(apiRequest, apiRequestPage);
            int syncBeanNum = 0;
            while (true) {
                if (CollectionUtils.isEmpty(apiResponse.getPagedData())) {
                    break;
                }
                List<UserBeanFlowInfo> userBeanFlowInfos = Lists.newArrayListWithCapacity(apiResponse.getPagedData().size());
                apiResponse.getPagedData().forEach(e -> {
                    UserBeanFlowInfo info = new UserBeanFlowInfo();
                    info.setUserId(wechatSyncTask.getUserId());
                    info.setOperationNum(e.getOperationNum());
                    info.setOperation(e.getOperation());
                    info.setOperationType(e.getOperationType());
                    info.setOperationTime(e.getOperationTime());
                    userBeanFlowInfos.add(info);
                });
                try {
                    userBeanFlowApiService.saveAll(userBeanFlowInfos);
                    weChatBeanFlowApiService.updateRecordAsSync(apiResponse.getPagedData().stream().map(UserBeanFlowInfo::getId).collect(Collectors.toList()));
                    syncBeanNum += apiResponse.getPagedData().stream().filter(e -> Operation.ADD.equals(e.getOperation())).mapToLong(UserBeanFlowInfo::getOperationNum).sum();
                    syncBeanNum -= apiResponse.getPagedData().stream().filter(e -> Operation.SUB.equals(e.getOperation())).mapToLong(UserBeanFlowInfo::getOperationNum).sum();
                } catch (Exception e) {
                    logger.info("同步金豆流水出现错误，taskId="+wechatSyncTask.getId(), e);
                    ifAssetSyncSuccess = false;
                }
                apiResponse = weChatBeanFlowApiService.findAll(apiRequest, apiRequestPage);
            }
            if (syncBeanNum > 0) {
                wechatSyncTask.setActualBean(wechatSyncTask.getActualBean() + syncBeanNum);
                if (wechatSyncTask.getActualBean().equals(wechatSyncTask.getPlanBean())) {
                    wechatSyncTask.setIfAssetSync(YesNoStatus.YES);
                } else {
                    ifAssetSyncSuccess = false;
                }
            }
        }

        boolean ifTaskSyncSuccess = true;
        if (YesNoStatus.NO.equals(wechatSyncTask.getIfTaskSync())) {
            ApiRequest apiRequest = ApiRequest.newInstance();
            apiRequest.filterEqual(QWeChatTaskOperationRecord.isSync, YesNoStatus.NO);
            apiRequest.filterEqual(QWeChatTaskOperationRecord.objectId, wechatSyncTask.getRelationId());

            ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
            apiRequestPage.paging(0, PAGE_SIZE);
            ApiResponse<WeChatTaskOperationRecord> apiResponse = weChatTaskOperationRecordApiService.findAll(apiRequest, apiRequestPage);
            if (CollectionUtils.isEmpty(apiResponse.getPagedData())) {
                wechatSyncTask.setIfTaskSync(YesNoStatus.YES);
            }
            while (true) {
                if (CollectionUtils.isEmpty(apiResponse.getPagedData())) {
                    if (ifTaskSyncSuccess) {
                        wechatSyncTask.setIfTaskSync(YesNoStatus.YES);
                    }
                    break;
                }
                List<UserTaskOperationRecord> userBeanFlowInfos = Lists.newArrayListWithCapacity(apiResponse.getPagedData().size());
                apiResponse.getPagedData().forEach(e -> {
                    UserTaskOperationRecord entity = new UserTaskOperationRecord();
                    entity.setOperationType(e.getOperationType());
                    entity.setBusinessId(e.getBusinessId());
                    entity.setOriginId(e.getOriginId());
                    entity.setOperation(e.getOperation());
                    entity.setOperationNum(e.getOperationNum());
                    entity.setAssetType(e.getAssetType());
                    entity.setObjectId(wechatSyncTask.getUserId().toString());
                    entity.setUserType(e.getUserType());
                    entity.setCreatedTime(e.getCreatedTime());
                    //产生金豆人统一存的是wechatRelation的id
                    entity.setOriginUserId(e.getOriginUserId());
                    userBeanFlowInfos.add(entity);
                });
                try {
                    userTaskOperationRecordApiService.saveAll(userBeanFlowInfos);
                    weChatTaskOperationRecordApiService.updateRecordAsSync(apiResponse.getPagedData().stream().map(WeChatTaskOperationRecord::getId).collect(Collectors.toList()));
                } catch (Exception e) {
                    logger.info("同步任务流水出现错误，taskId="+wechatSyncTask.getId(), e);
                    ifTaskSyncSuccess = false;
                }
                apiResponse = weChatTaskOperationRecordApiService.findAll(apiRequest, apiRequestPage);
            }
        }

        boolean ifShareSyncSuccess = true;
        if (YesNoStatus.NO.equals(wechatSyncTask.getIfShareSync())) {
            ApiRequest apiRequest = ApiRequest.newInstance();
            apiRequest.filterEqual(QShareRecord.isSync, YesNoStatus.NO);
            apiRequest.filterEqual(QShareRecord.userId, wechatSyncTask.getRelationId());
            apiRequest.filterEqual(QShareRecord.userType, UserIdType.NEWS_SMALL_PROGRAM);

            ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
            apiRequestPage.paging(0, PAGE_SIZE);
            ApiResponse<ShareRecord> apiResponse = shareRecordApiService.findAll(apiRequest, apiRequestPage);
            if (CollectionUtils.isEmpty(apiResponse.getPagedData())) {
                wechatSyncTask.setIfShareSync(YesNoStatus.YES);
            }
            while (true) {
                if (CollectionUtils.isEmpty(apiResponse.getPagedData())) {
                    if (ifShareSyncSuccess) {
                        wechatSyncTask.setIfShareSync(YesNoStatus.YES);
                    }
                    break;
                }
                List<ShareRecord> shareRecords = Lists.newArrayListWithCapacity(apiResponse.getPagedData().size());
                apiResponse.getPagedData().forEach(e -> {
                    ShareRecord entity = new ShareRecord();
                    entity.setUserId(wechatSyncTask.getUserId());
                    entity.setUserType(UserIdType.APP);
                    entity.setShareType(e.getShareType());
                    entity.setPlatformType(e.getPlatformType());
                    entity.setSourceId(e.getSourceId());
                    entity.setCreatedTime(e.getCreatedTime());
                    entity.setUpdatedTime(e.getUpdatedTime());
                    shareRecords.add(entity);
                });
                try {
                    shareRecordApiService.saveAll(shareRecords);
                    shareRecordApiService.updateRecordAsSync(apiResponse.getPagedData().stream().map(ShareRecord::getId).collect(Collectors.toList()));
                } catch (Exception e) {
                    logger.info("同步金豆流水出现错误，taskId="+wechatSyncTask.getId(), e);
                    ifShareSyncSuccess = false;
                }
                apiResponse = shareRecordApiService.findAll(apiRequest, apiRequestPage);
            }
        }
        wechatSyncTaskApiService.update(wechatSyncTask);
        return ifAssetSyncSuccess && ifShareSyncSuccess && ifTaskSyncSuccess;
    }
}
