package cn.lehome.dispatcher.utils.task;

import cn.lehome.base.api.advertising.bean.activity.MasterApprenticeRelationship;
import cn.lehome.base.api.advertising.bean.task.DailyConversionAccount;
import cn.lehome.base.api.advertising.bean.task.QUserTaskOperationRecord;
import cn.lehome.base.api.advertising.bean.task.UserTaskOperationRecord;
import cn.lehome.base.api.advertising.service.activity.MasterApprenticeRelationshipApiService;
import cn.lehome.base.api.advertising.service.task.DailyConversionAccountApiService;
import cn.lehome.base.api.advertising.service.task.UserTaskOperationRecordApiService;
import cn.lehome.base.api.content.bean.comment.CommentInfo;
import cn.lehome.base.api.content.bean.post.LikesInfo;
import cn.lehome.base.api.content.service.comment.CommentInfoApiService;
import cn.lehome.base.api.content.service.post.LikesInfoApiService;
import cn.lehome.base.api.tool.bean.job.ScheduleJob;
import cn.lehome.base.api.tool.service.job.ScheduleJobApiService;
import cn.lehome.bean.advertising.enums.task.AssetType;
import cn.lehome.bean.advertising.enums.task.ConsumeType;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.CoreDateUtils;
import cn.lehome.framework.bean.core.enums.Operation;
import cn.lehome.framework.bean.core.enums.OperationType;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * @author yanwenkai
 * @date 2018/5/23
 */
@Service
public class DailyConversionAccountServiceImpl implements DailyConversionAccountService {

    @Autowired
    private ScheduleJobApiService scheduleJobApiService;

    @Autowired
    private UserTaskOperationRecordApiService userTaskOperationRecordApiService;

    @Autowired
    private CommentInfoApiService commentInfoApiService;

    @Autowired
    private LikesInfoApiService likesInfoApiService;

    @Autowired
    private MasterApprenticeRelationshipApiService relationshipApiService;

    @Autowired
    private DailyConversionAccountApiService dailyConversionAccountApiService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public void updateDailyInviteReward(){
        ApiRequest apiRequest = ApiRequest.newInstance();
//        apiRequest.filterLessEqual("id",10);
        List<DailyConversionAccount> list = dailyConversionAccountApiService.findAll(apiRequest);
        System.out.println("奖励现金共需处理【"+list.size()+"】条数据");
        int i = 0;
        for (DailyConversionAccount account : list) {
            Date createdTime = account.getCreatedTime();
            Date start = getYesterdayTime(createdTime, YesNoStatus.YES, 1);
            Date end = getYesterdayTime(createdTime, YesNoStatus.NO, 1);
            Long sumInviteReward = userTaskOperationRecordApiService.sumInviteReward(OperationType.INVITE_FRIEND_V2, start, end);
            if (sumInviteReward == null) {
                sumInviteReward = 0L;
            }
            account.setInviteRewardByDay(sumInviteReward);
            dailyConversionAccountApiService.update(account);
            i++;
        }
        System.out.println("奖励现金数据处理完毕，共处理【"+i+"】条数据");
    }

    public static void main(String[] args) {
        Date yesterdayTime = getYesterdayTime(new Date(), YesNoStatus.YES, 1);
        Date endTime = getYesterdayTime(new Date(), YesNoStatus.NO, 1);
        String s = CoreDateUtils.formatDateTime(yesterdayTime);
        String s1 = CoreDateUtils.formatDateTime(endTime);
        System.out.println(s);
        System.out.println(s1);
    }

    public static Date getYesterdayTime(Date date,YesNoStatus yesNoStatus,Integer amount){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, amount);
        if (YesNoStatus.YES.equals(yesNoStatus)) {
            calendar.set(Calendar.HOUR_OF_DAY,0);
            calendar.set(Calendar.SECOND,0);
            calendar.set(Calendar.MINUTE,0);
            calendar.set(Calendar.MILLISECOND,0);
            return calendar.getTime();
        }
        calendar.set(Calendar.HOUR_OF_DAY,23);
        calendar.set(Calendar.MINUTE,59);
        calendar.set(Calendar.SECOND,59);
        calendar.set(Calendar.MILLISECOND,999);
        return calendar.getTime();
    }

    /**
     * 修改徒弟进贡和邀请好友任务userTaskOperationRecord中originUserId
     */
    @Override
    public void updateApprenticeOperationRecord() {
        ApiRequest apiRequest = ApiRequest.newInstance();
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        int page = 0;
        Boolean flag = true;
        while (flag) {
            apiRequestPage.paging(page, 20);
            ApiResponse<MasterApprenticeRelationship> all = relationshipApiService.findAll(apiRequest, apiRequestPage);
            System.out.println("page===>" + page + ",count===>" + all.getCount());
            if (all.getCount() == 0) {
                flag = false;
            } else {
                page++;
                Collection<MasterApprenticeRelationship> pagedData = all.getPagedData();
                Iterator<MasterApprenticeRelationship> iterator = pagedData.iterator();
                while (iterator.hasNext()) {
                    MasterApprenticeRelationship next = iterator.next();
                    Long apprenticeId = next.getApprenticeId();
                    List<Object[]> list = userTaskOperationRecordApiService.findAllByApprenticeId(apprenticeId);
                    for (int i = 0; i < list.size(); i++) {
                        Object[] objects = list.get(i);
                        String dateStr = (String) objects[0];
                        dateStr += " 00:00:00";
                        Date date = CoreDateUtils.parseDateTime(dateStr);
                        Long sumBean = 0L;
                        if (objects[1] instanceof BigInteger) {
                            BigInteger object = (BigInteger) objects[1];
                            sumBean = object.longValue();
                        } else if (objects[1] instanceof BigDecimal) {
                            BigDecimal object = (BigDecimal) objects[1];
                            sumBean = object.longValue();
                        } else {
                            sumBean = (Long) objects[1];
                        }
                        if (sumBean >= 100) {
                            Long conversionBean = sumBean / 10L;
                            ApiRequest apiRequest1 = ApiRequest.newInstance();
                            apiRequest1.filterEqual(QUserTaskOperationRecord.objectId, next.getMasterId());
                            apiRequest1.filterLessEqual(QUserTaskOperationRecord.createdTime, getLastDate(date));
                            apiRequest1.filterGreaterEqual(QUserTaskOperationRecord.createdTime, date);
                            apiRequest1.filterIn(QUserTaskOperationRecord.originUserId, Lists.newArrayList("", "0"));
                            apiRequest1.filterEqual(QUserTaskOperationRecord.operationNum, conversionBean);
                            apiRequest1.filterEqual(QUserTaskOperationRecord.operationType, OperationType.SUBORDINATE_CONTRIBUTION);
                            ApiRequestPage apiRequestPage1 = ApiRequestPage.newInstance();
                            apiRequestPage1.paging(0, 1000);
                            ApiResponse<UserTaskOperationRecord> beanAll = userTaskOperationRecordApiService.findAll(apiRequest1, apiRequestPage1);
                            beanOrMoneyUpdateRecord(apprenticeId, beanAll);
                            apiRequest1 = ApiRequest.newInstance();
                            apiRequest1.filterEqual(QUserTaskOperationRecord.objectId, next.getMasterId());
                            apiRequest1.filterLessEqual(QUserTaskOperationRecord.createdTime, getLastDate(date));
                            apiRequest1.filterGreaterEqual(QUserTaskOperationRecord.createdTime, date);
                            apiRequest1.filterIn(QUserTaskOperationRecord.originUserId, Lists.newArrayList("", "0"));
                            apiRequest1.filterEqual(QUserTaskOperationRecord.operationNum, 100);
                            apiRequest1.filterEqual(QUserTaskOperationRecord.operationType, OperationType.INVITE_FRIEND_V2);
                            ApiResponse<UserTaskOperationRecord> rewardMoneyRecords = userTaskOperationRecordApiService.findAll(apiRequest1, apiRequestPage1);
                            beanOrMoneyUpdateRecord(apprenticeId, rewardMoneyRecords);
                        }
                    }
                }
            }
        }
        System.out.println("修改徒弟进贡和邀请好友任务userTaskOperationRecord中originUserId完毕。。。。。");
    }

    private void beanOrMoneyUpdateRecord(Long apprenticeId, ApiResponse<UserTaskOperationRecord> rewardMoneyRecords) {
        if (rewardMoneyRecords.getCount() > 0) {
            List<UserTaskOperationRecord> records = (List<UserTaskOperationRecord>) rewardMoneyRecords.getPagedData();
            UserTaskOperationRecord userTaskOperationRecord = records.get(0);
            userTaskOperationRecord.setOriginUserId(apprenticeId.toString());
            userTaskOperationRecordApiService.update(userTaskOperationRecord);
        }
    }

    public Date getLastDate(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR_OF_DAY, 23);
        calendar.add(Calendar.MINUTE, 59);
        calendar.add(Calendar.SECOND, 59);
        calendar.add(Calendar.MILLISECOND, 99);
        return calendar.getTime();
    }

    /**
     * 修改排除徒弟进贡和邀请好友任务userTaskOperationRecord中originUserId
     */
    @Override
    public void updateTaskOperationRecord() {
        Boolean flag = true;
        ApiRequest apiRequest = ApiRequest.newInstance();
        apiRequest.filterIn(QUserTaskOperationRecord.originUserId, Lists.newArrayList("", "0"))
                .filterNotIn(QUserTaskOperationRecord.operationType,Lists.newArrayList(OperationType.INVITE_FRIEND_V2,OperationType.SUBORDINATE_CONTRIBUTION));;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(0, 1000);
        Long total = 0L;
        while (flag) {
            ApiResponse<UserTaskOperationRecord> all = userTaskOperationRecordApiService.findAll(apiRequest, apiRequestPage);
            if (all.getTotal() == total) {
                flag = false;
            }
            total = all.getTotal();
            System.out.println("count===>" + all.getCount() + ",total===>" + all.getTotal());
            if (all.getCount() == 0) {
                flag = false;
            } else {
                Collection<UserTaskOperationRecord> pagedData = all.getPagedData();
                Iterator<UserTaskOperationRecord> iterator = pagedData.iterator();
                while (iterator.hasNext()) {
                    UserTaskOperationRecord next = iterator.next();
                    OperationType operationType = next.getOperationType();
                    switch (operationType) {
                        case POST_INDEX:
                            next.setOriginUserId("0000");
                            break;
                        case POST_TOP:
                            next.setOriginUserId("0000");
                            break;
                        case DEL_POST_MANAGER:
                            next.setOriginUserId("0000");
                            break;
                        case SIGN_IN:
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case DRAW_CARD:
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case STEAL_CARD:
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case DEL_POST_SELF:
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case LOGIN_AUTH_V2:
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case RETURN_REWARD:
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case FIRST_OPEN_DOOR_V2:
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case COMPLETE_USER_DATA_V2:
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case BROWSE_NEWS:
                            //看新闻
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case OPEN_SYSTEM_NOTIFY_V2:
                            //打开系统通知
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case DRAW_CARD_BEAN:
                            //抽卡机会转现金
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case BEAN_GIFTS:
                            //金豆礼包
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case BEAN_CONVERT_MONEY:
                            //金豆转现金
                            next.setOriginUserId(next.getObjectId());
                            break;
                        case POST_AGREE:
                            if (StringUtils.isNotEmpty(next.getOriginId())) {
                                LikesInfo one = likesInfoApiService.findOne(Long.valueOf(next.getOriginId()));
                                if (one != null) {
                                    next.setOriginUserId(one.getUserId().toString());
                                }
                            }
                            break;
                        case AGREE_REPLY:
                            if (StringUtils.isNotEmpty(next.getOriginId())) {
                                CommentInfo one1 = commentInfoApiService.findOne(next.getOriginId());
                                if (one1 != null) {
                                    next.setOriginUserId(one1.getCommentUserId().toString());
                                }
                            }
                            break;
                        case POST_COMMENT:
                            if (StringUtils.isNotEmpty(next.getOriginId())) {
                                CommentInfo one2 = commentInfoApiService.findOne(next.getOriginId());
                                if (one2 != null) {
                                    next.setOriginUserId(one2.getCommentUserId().toString());
                                }
                            }
                            break;
                        case SUBORDINATE_CONTRIBUTION:
                            //徒弟进贡
                            break;
                        case INVITE_FRIEND_V2:
                            //邀请好友
                            break;
                        default:
                            System.out.println("error.........");
                            break;
                    }
                    userTaskOperationRecordApiService.update(next);
                }
            }
        }
        System.out.println("修改排除徒弟进贡和邀请好友任务userTaskOperationRecord中originUserId完毕。。。。。。。");
    }

    @Override
    public void dailyConversionAccountStatistics() {

        String cronExpression = String.format("%s %s %s %s %s %s", 0, 0, 1, "*", "*", "?");
        ScheduleJob scheduleJob = new ScheduleJob();
        scheduleJob.setIsSync(YesNoStatus.YES);
        scheduleJob.setIsOnce(YesNoStatus.NO);
        scheduleJob.setJobGroup("dailyConversionAccount-statistics-group");
        scheduleJob.setJobName("dailyConversionAccount-statistics-job");
        scheduleJob.setAliasName("每日金豆金额兑换统计");
        scheduleJob.setCronExpression(cronExpression);
        scheduleJob.setDescription("每日金豆金额兑换统计");
        scheduleJob.setExeParams("");
        scheduleJob.setExeServiceName("dailyConversionAccountJobService");
        scheduleJobApiService.create(scheduleJob);
    }

    String PRIZE_DATA_PREFIX = "advert.prize.data:";

    @Override
    public void refreshPrizeRedis(String[] input){
        if (input.length < 2) {
            System.out.println("输入参数有误，请重新输入");
            return;
        }
        Long advertId = Long.valueOf(input[1]);
        String key = String.format("%s%s", PRIZE_DATA_PREFIX, advertId);
        if (stringRedisTemplate.hasKey(key)) {
            HashOperations<String, String, String> operations = stringRedisTemplate.opsForHash();
            Map<String, String> entries = operations.entries(key);
            Set<String> set = entries.keySet();
            for (String index : set) {
                String value = entries.get(index);
                if ("50000".equals(value) && operations.hasKey(key, index)) {
                    operations.put(key,index,"5000");
                    System.out.println("key:" + index +",value:" + value + "修改为5000完成");
                }
            }
        }
    }
}
