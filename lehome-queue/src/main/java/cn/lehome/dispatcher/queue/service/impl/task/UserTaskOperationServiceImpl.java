package cn.lehome.dispatcher.queue.service.impl.task;

import cn.lehome.base.api.business.activity.bean.task.TaskSetting;
import cn.lehome.base.api.business.activity.bean.task.UserTaskAccount;
import cn.lehome.base.api.business.activity.bean.task.UserTaskOperationRecord;
import cn.lehome.base.api.business.activity.constant.PubConstant;
import cn.lehome.base.api.business.activity.service.task.TaskSettingApiService;
import cn.lehome.base.api.business.activity.service.task.UserTaskAccountApiService;
import cn.lehome.base.api.business.activity.service.task.UserTaskOperationRecordApiService;
import cn.lehome.base.api.user.bean.asset.UserBeanFlowInfo;
import cn.lehome.base.api.user.bean.asset.UserDepositFlowInfo;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.asset.UserAssetApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.business.activity.enums.task.AssetType;
import cn.lehome.bean.business.activity.enums.task.ConsumeType;
import cn.lehome.bean.business.activity.enums.task.LimitType;
import cn.lehome.bean.business.activity.enums.task.TaskType;
import cn.lehome.dispatcher.queue.bean.UserOperationRecord;
import cn.lehome.dispatcher.queue.exception.task.UserTaskException;
import cn.lehome.dispatcher.queue.service.impl.AbstractBaseServiceImpl;
import cn.lehome.dispatcher.queue.service.task.UserTaskOperationService;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import cn.lehome.framework.bean.core.enums.UserType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by zuoguodong on 2018/5/16
 */
@Service
public class UserTaskOperationServiceImpl extends AbstractBaseServiceImpl implements UserTaskOperationService {

    @Autowired
    UserAssetApiService userAssetApiService;

    @Autowired
    UserTaskAccountApiService userTaskAccountApiService;

    @Autowired
    UserTaskOperationRecordApiService userTaskOperationRecordApiService;

    @Autowired
    UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    TaskSettingApiService taskSettingApiService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Value("${post.user.operation.expire.times}")
    private Long expireTimes;

    @Override
    public void saveUserOperation(UserOperationRecord userOperationRecord) {
        //保存用户资产数据 领取次数不为空时,消耗金豆时才保存资产数据
        if(userOperationRecord.getDrawCount()!=null || ConsumeType.get(userOperationRecord.getOperationType().getValue())!=null) {
            if (userOperationRecord.getAssetType().equals(AssetType.BEAN)) {
                UserBeanFlowInfo userBeanFlowInfo = new UserBeanFlowInfo();
                userBeanFlowInfo.setUserId(Long.valueOf(userOperationRecord.getObjectId()));
                userBeanFlowInfo.setOperation(userOperationRecord.getOperation());
                userBeanFlowInfo.setOperationNum(userOperationRecord.getOperationNum());
                userBeanFlowInfo.setOperationType(userOperationRecord.getOperationType());
                userAssetApiService.operateBeanNum(userBeanFlowInfo);
            } else if (userOperationRecord.getAssetType().equals(AssetType.MONEY)) {
                UserDepositFlowInfo userDepositFlowInfo = new UserDepositFlowInfo();
                userDepositFlowInfo.setUserId(Long.valueOf(userOperationRecord.getObjectId()));
                userDepositFlowInfo.setOperation(userOperationRecord.getOperation());
                userDepositFlowInfo.setOperationNum(userOperationRecord.getOperationNum());
                userDepositFlowInfo.setOperationType(userOperationRecord.getOperationType());
                userAssetApiService.operateDepositNum(userDepositFlowInfo);
            } else {
                throw new UserTaskException("资产类型不正确");
            }
        }
        //保存用户操作数据 只有任务时会才执行保存
        TaskType taskType = TaskType.get(userOperationRecord.getOperationType().getValue());
        if(taskType != null) {
            UserTaskAccount userTaskAccount = new UserTaskAccount();
            userTaskAccount.setTaskType(taskType);
            userTaskAccount.setCommunityId(userOperationRecord.getCommunityId());
            userTaskAccount.setStatus(userOperationRecord.getUserTaskStatus());
            userTaskAccount.setAmount(userOperationRecord.getAmount());
            userTaskAccount.setBeanAmount(userOperationRecord.getBeanAmount());
            userTaskAccount.setCompleteCount(userOperationRecord.getCompleteCount());
            userTaskAccount.setDrawAmount(userOperationRecord.getDrawAmount());
            userTaskAccount.setDrawBeanAmount(userOperationRecord.getDrawBeanAmount());
            userTaskAccount.setDrawCount(userOperationRecord.getDrawCount());
            userTaskAccount.setCompleteTime(userOperationRecord.getCompleteTime());
            userTaskAccount.setDrewStatus(userOperationRecord.getDrewStatus());
            if(userOperationRecord.getPhone() != null) {
                userTaskAccount.setPhone(userOperationRecord.getPhone());
            }else{
                if(userOperationRecord.getUserType().equals(UserType.USER)) {
                    UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(Long.valueOf(userOperationRecord.getObjectId()));
                    if(userInfoIndex == null){
                        throw new UserTaskException("用户不存在");
                    }
                    userTaskAccount.setPhone(userInfoIndex.getPhone());
                }
            }
            userTaskAccountApiService.save(userTaskAccount);
        }

        //保存用户操作流水
        UserTaskOperationRecord userTaskOperationRecord = BeanMapping.map(userOperationRecord,UserTaskOperationRecord.class);
        userTaskOperationRecordApiService.save(userTaskOperationRecord);
    }

    @Override
    public TaskSetting getPostCommentReward(Long userId, String commentUserId, String postId) {
        TaskSetting taskSetting = getTaskSetting(TaskType.POST_COMMENT);
        //判断任务是否有效
        if(taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)){
            return null;
        }
        //单贴收益是否超过上限
        String postCommentRewardKey = PubConstant.POST_COMMENT_REWARD_KEY_PREFIX + postId;
        if(isGreatThanLimit(postCommentRewardKey,taskSetting,100)){
            return null;
        }
        String postCommentUser = PubConstant.POST_COMMENT_USER_KEY_PREFIX + commentUserId;
        if(isMember(userId.toString(),postCommentUser)){
            return null;
        }
        //是否超过论坛最大上限
        if(isGreatThanPostMaxReward(userId,taskSetting)){
            return null;
        }
        //是否自己给自己评论
        if(userId.toString().equals(commentUserId)){
            return null;
        }
        return taskSetting;
    }

    @Override
    public void setPostCommentRewardCache(Long userId, String commentUserId, String postId,TaskSetting taskSetting) {
        String postCommentRewardKey = PubConstant.POST_COMMENT_REWARD_KEY_PREFIX + postId;
        addReward(postCommentRewardKey,taskSetting);
        String postCommentUser = PubConstant.POST_COMMENT_USER_KEY_PREFIX + commentUserId;
        addItem(userId.toString(),postCommentUser);
        addUserPostSumReward(userId,taskSetting);
    }

    @Override
    public TaskSetting getPostAgreeReward(Long userId, Long agreeUserId, String postId) {
        TaskSetting taskSetting = getTaskSetting(TaskType.POST_AGREE);
        //判断任务是否有效
        if(taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)){
            return null;
        }
        //单贴收益是否超过上限
        String postAgreeRewardKey = PubConstant.POST_AGREE_REWARD_KEY_PREFIX + postId;
        if(isGreatThanLimit(postAgreeRewardKey,taskSetting,100)){
            return null;
        }
        //评论用户是否对同一贴子进行点赞
        String postAgreeUser = PubConstant.POST_AGREE_USER_KEY_PREFIX + postId;
        if(isMember(userId.toString(),postAgreeUser)){
            logger.warn("同一成员评论不得豆");
            return null;
        }
        //是否超过论坛最大上限
        if(isGreatThanPostMaxReward(userId,taskSetting)){
            return null;
        }
        //是否自己给自己评论
        if(userId.equals(agreeUserId)){
            logger.warn("自己对自己操作不得豆");
            return null;
        }
        return taskSetting;
    }

    @Override
    public TaskSetting getPostCommentAgreeReward(Long userId, Long agreeUserId, String commentId) {
        TaskSetting taskSetting = getTaskSetting(TaskType.POST_COMMENT_AGREE);
        //判断任务是否有效
        if(taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)){
            return null;
        }
        //单贴收益是否超过上限
        String postAgreeRewardKey = PubConstant.POST_COMMENT_AGREE_REWARD_KEY_PREFIX + commentId;
        if(isGreatThanLimit(postAgreeRewardKey, taskSetting,100)){
            return null;
        }
        //点赞用户是否对同一评论进行点赞
        String postAgreeUser = PubConstant.POST_COMMENT_USER_KEY_PREFIX + commentId;
        if(isMember(userId.toString(), postAgreeUser)){
            logger.warn("同一成员点赞不得豆");
            return null;
        }
        //是否超过论坛最大上限
        if(isGreatThanPostMaxReward(userId,taskSetting)){
            return null;
        }
        //是否自己给自己评论
        if(userId.equals(agreeUserId)){
            logger.warn("自己对自己操作不得豆");
            return null;
        }
        return taskSetting;
    }

    @Override
    public void setPostAgreeRewardCache(Long userId, Long agreeUserId, String postId, TaskSetting taskSetting) {
        String postAgreeRewardKey = PubConstant.POST_AGREE_REWARD_KEY_PREFIX + postId;
        addReward(postAgreeRewardKey,taskSetting);
        String postAgreeUser = PubConstant.POST_AGREE_USER_KEY_PREFIX + agreeUserId.toString();
        addItem(userId.toString(),postAgreeUser);
        addUserPostSumReward(userId,taskSetting);
    }

    @Override
    public void setPostCommentAgreeRewardCache(Long userId, Long agreeUserId, String commentId, TaskSetting taskSetting) {
        String postAgreeRewardKey = PubConstant.POST_COMMENT_AGREE_REWARD_KEY_PREFIX + commentId;
        addReward(postAgreeRewardKey,taskSetting);
        String postAgreeUser = PubConstant.POST_COMMENT_AGREE_USER_KEY_PREFIX + agreeUserId.toString();
        addItem(userId.toString(), postAgreeUser);
        addUserPostSumReward(userId, taskSetting);
    }

    @Override
    public TaskSetting getPostReplyReward(Long userId, String replyUserId, String beCommentId) {
        TaskSetting taskSetting = getTaskSetting(TaskType.AGREE_REPLY);
        //判断任务是否有效
        if(taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)){
            return null;
        }
        //单贴收益是否超过上限
        String commentReplyRewardKey = PubConstant.COMMENT_REPLY_REWARD_KEY_PREFIX + beCommentId;
        if(isGreatThanLimit(commentReplyRewardKey,taskSetting,50)){
            return null;
        }
        //评论用户是否对同一贴子进行点赞
        String commentReplyUser = PubConstant.COMMENT_REPLY_USER_KEY_PREFIX + userId.toString();
        if(isMember(userId.toString(),commentReplyUser)){
            return null;
        }
        //是否超过论坛最大上限
        if(isGreatThanPostMaxReward(userId,taskSetting)){
            return null;
        }
        //是否自己给自己评论
        if(userId.toString().equals(replyUserId)){
            return null;
        }
        return taskSetting;
    }

    @Override
    public void setPostReplyRewardCache(Long userId, String replyUserId, String beCommentId, TaskSetting taskSetting) {
        String postAgreeRewardKey = PubConstant.COMMENT_REPLY_REWARD_KEY_PREFIX + beCommentId;
        addReward(postAgreeRewardKey,taskSetting);
        String postAgreeUser = PubConstant.COMMENT_REPLY_USER_KEY_PREFIX + replyUserId;
        addItem(userId.toString(),postAgreeUser);
        addUserPostSumReward(userId,taskSetting);
    }


    public TaskSetting getTaskSetting(TaskType taskType){
        return taskSettingApiService.findByTaskType(taskType);
    }

    @Override
    public Long getPostIncome(Long userId, TaskSetting taskSetting) {
        String userPostRewardSumKey = PubConstant.POST_REWARD_SUM_KEY_PREFIX + userId;
        if(stringRedisTemplate.hasKey(userPostRewardSumKey)){
            HashOperations<String,String,String> hashOperations = stringRedisTemplate.opsForHash();
            Map<String,String> map = hashOperations.entries(userPostRewardSumKey);
            if(map.containsKey(taskSetting.getLimitUnit().name())){
                String rewardNumStr = map.get(taskSetting.getLimitUnit().name());
                Long rewardNum = Long.valueOf(rewardNumStr);
                return rewardNum;
            }
        }
        return 0l;
    }

    private Long getExpireTimes(){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.set(Calendar.HOUR_OF_DAY,23);
        calendar.set(Calendar.MINUTE,59);
        calendar.set(Calendar.SECOND,59);
        return (calendar.getTime().getTime() - System.currentTimeMillis()) / 1000;
    }

    /**
     * 判断当前key，当前限制类型，是否超过限制数
     * 如果限制数为-1时为不限制
     * @param key
     * @param taskSetting
     * @param maxTimes
     * @return
     */
    private boolean isGreatThanLimit(String key,TaskSetting taskSetting,Integer maxTimes){
        if(stringRedisTemplate.hasKey(key)){
            HashOperations<String,String,String> hashOperations = stringRedisTemplate.opsForHash();
            Map<String,String> map = hashOperations.entries(key);
            if(map.containsKey(taskSetting.getLimitUnit().name())){
                String rewardNumStr = map.get(taskSetting.getLimitUnit().name());
                Long rewardNum = Long.valueOf(rewardNumStr);
                if(rewardNum >= maxTimes && !maxTimes.equals(-1)){
                    hashOperations.put(key,PubConstant.TASK_FINISH_FLAG,PubConstant.TASK_FINISH_YES);
                    logger.warn("超过单贴最大上限");
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 判断当前人 限制类型 限制数
     * @param userId
     * @param taskSetting
     * @return
     */
    private boolean isGreatThanPostMaxReward(Long userId,TaskSetting taskSetting){
        String userPostRewardSumKey = PubConstant.POST_REWARD_SUM_KEY_PREFIX + userId;
        if(stringRedisTemplate.hasKey(userPostRewardSumKey)){
            HashOperations<String,String,String> hashOperations = stringRedisTemplate.opsForHash();
            Map<String,String> map = hashOperations.entries(userPostRewardSumKey);
            if(map.containsKey(taskSetting.getLimitUnit().name())){
                String rewardNumStr = map.get(taskSetting.getLimitUnit().name());
                Long rewardNum = Long.valueOf(rewardNumStr);
                if(rewardNum >= taskSetting.getMaxTimes() && !taskSetting.getMaxTimes().equals(-1)){
                    hashOperations.put(userPostRewardSumKey,PubConstant.TASK_FINISH_FLAG,PubConstant.TASK_FINISH_YES);
                    logger.warn("超过论坛最大上限");
                    return true;
                }
            }
        }
        return false;
    }

    private void addUserPostSumReward(Long userId,TaskSetting taskSetting){
        String userPostRewardSumKey = PubConstant.POST_REWARD_SUM_KEY_PREFIX + userId;
        HashOperations<String,String,String> hashOperations = stringRedisTemplate.opsForHash();
        Map<String,String> map = hashOperations.entries(userPostRewardSumKey);
        if(map.size() == 0){
            hashOperations.put(userPostRewardSumKey,taskSetting.getLimitUnit().name(),taskSetting.getRewardAmount().toString());
            stringRedisTemplate.expire(userPostRewardSumKey, getExpireTimes(), TimeUnit.SECONDS);
        }else{
            String rewardNumStr = map.get(taskSetting.getLimitUnit().name());
            Long rewardNum = Long.valueOf(rewardNumStr);
            rewardNum += taskSetting.getRewardAmount();
            hashOperations.put(userPostRewardSumKey,taskSetting.getLimitUnit().name(),rewardNum.toString());
        }
    }

    /**
     * 设置 当前key 当前限制单位 限制数
     * 如果限制类型为每天的，设置过期时间
     * @param key
     * @param taskSetting
     */
    private void addReward(String key,TaskSetting taskSetting){
        HashOperations<String,String,String> hashOperations = stringRedisTemplate.opsForHash();
        Map<String,String> map = hashOperations.entries(key);
        if(map.size() == 0){
            hashOperations.put(key,taskSetting.getLimitUnit().name(),taskSetting.getRewardAmount().toString());
            //如果是以天为单位的任务需要为KEY加过期设置
            if(taskSetting.getLimitType().equals(LimitType.DAILY)) {
                stringRedisTemplate.expire(key, getExpireTimes(), TimeUnit.SECONDS);
            }
        }else{
            String rewardNumStr = map.get(taskSetting.getLimitUnit().name());
            Long cacheNum = Long.valueOf(rewardNumStr);
            cacheNum += taskSetting.getRewardAmount();
            hashOperations.put(key,taskSetting.getLimitUnit().name(),cacheNum.toString());
        }
    }

    private void addItem(String user,String item){
        String key = PubConstant.POST_USER_KEY_PREFIX + user;
        if(!stringRedisTemplate.hasKey(key)) {
            stringRedisTemplate.opsForSet().add(key, item);
            stringRedisTemplate.expire(key,expireTimes,TimeUnit.SECONDS);
        }else{
            stringRedisTemplate.opsForSet().add(key, item);
        }
    }

    private boolean isMember(String user,String value){
        String key = PubConstant.POST_USER_KEY_PREFIX + user;
        if(stringRedisTemplate.opsForSet().isMember(key,value)){
            return true;
        }
        return false;
    }
}
