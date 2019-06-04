package cn.lehome.dispatcher.queue.listener.activity;


import cn.lehome.base.api.advertising.bean.task.TaskSetting;
import cn.lehome.base.api.advertising.bean.task.UserTaskAccount;
import cn.lehome.base.api.advertising.constant.JoinActivityTypeConstants;
import cn.lehome.base.api.advertising.constant.JoinRewardActivityTypeConstants;
import cn.lehome.base.api.advertising.constant.PubConstant;
import cn.lehome.base.api.advertising.service.task.UserTaskAccountApiService;
import cn.lehome.base.api.common.bean.device.ClientDeviceIndex;
import cn.lehome.base.api.common.event.JoinActivityEventBean;
import cn.lehome.base.api.common.service.device.ClientDeviceIndexApiService;
import cn.lehome.base.api.content.bean.comment.CommentInfoIndex;
import cn.lehome.base.api.content.bean.post.PostInfoIndex;
import cn.lehome.base.api.content.service.comment.CommentInfoIndexApiService;
import cn.lehome.base.api.content.service.post.PostInfoIndexApiService;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.advertising.enums.task.*;
import cn.lehome.bean.user.entity.enums.user.SexType;
import cn.lehome.dispatcher.queue.bean.UserOperationRecord;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.task.UserTaskOperationService;
import cn.lehome.framework.base.api.core.compoment.redis.lock.RedisLock;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.exception.common.RedisLockFailException;
import cn.lehome.framework.bean.core.enums.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Created by zuoguodong on 2018/5/16.
 */
public class JoinRewardActivityListener extends AbstractJobListener {

    private static final Integer IOS_MIN_VERSION_CODE = 320;
    private static final Integer ANDROID_MIN_VERSION_CODE = 3200;

    @Autowired
    UserTaskOperationService userTaskOperationService;

    @Autowired
    CommentInfoIndexApiService commentInfoIndexApiService;

    @Autowired
    PostInfoIndexApiService postInfoIndexApiService;

    @Autowired
    UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    UserTaskAccountApiService userTaskAccountApiService;

    @Autowired
    StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ClientDeviceIndexApiService clientDeviceIndexApiService;

    @Override
    public void execute(IEventMessage eventMessage) {
        SimpleEventMessage<JoinActivityEventBean> simpleEventMessage = (SimpleEventMessage<JoinActivityEventBean>) eventMessage;
        JoinActivityEventBean joinActivityEventBean = simpleEventMessage.getData();
        List<Object> attributes = joinActivityEventBean.getAttributes();
        switch (joinActivityEventBean.getJoinActivityType()) {
            case JoinRewardActivityTypeConstants.SIGN_IN:
                logger.info("用户签到获奖励");
                signIn(attributes);
                break;
            case JoinActivityTypeConstants.UPDATE_USER_DATA:
                logger.info("完善资料获奖励");
                updateUserData(attributes);
                break;
            case JoinActivityTypeConstants.FIRST_OPEN_DOOR:
                logger.info("首次开门获奖励");
                openDoor(attributes);
                break;
            case JoinActivityTypeConstants.USER_AUTH_HOUSE:
                logger.info("登录认证获奖励");
                authHouse(attributes);
                break;
            case JoinRewardActivityTypeConstants.POST_COMMENT:
                logger.info("发贴评论获奖励");
                postComment(attributes);
                break;
            case JoinRewardActivityTypeConstants.POST_AGREE:
                logger.info("发贴点赞获奖励");
                postAgree(attributes);
                break;
            case JoinRewardActivityTypeConstants.POST_COMMENT_AGREE:
                logger.info("评论被点赞获奖励");
                postCommentAgree(attributes);
                break;
            case JoinRewardActivityTypeConstants.AGREE_REPLY:
                logger.info("评论回复获奖励");
                postReply(attributes);
                break;
            case JoinRewardActivityTypeConstants.POST_TOP:
                logger.info("贴子被置顶获奖励");
                postTop(attributes);
                break;
            case JoinRewardActivityTypeConstants.POST_INDEX:
                logger.info("贴子推荐到首页获奖励");
                postIndex(attributes);
                break;
            case JoinRewardActivityTypeConstants.SUBORDINATE_CONTRIBUTION:
                logger.info("徒弟进贡奖励");
                contribution(attributes);
                break;
            case JoinRewardActivityTypeConstants.INVITE_FRIEND_V2:
                logger.info("邀请好友奖励");
                inviteFriend(attributes);
                break;
            case JoinRewardActivityTypeConstants.POST_SELECTED:
                logger.info("帖子加精");
                postSelected(attributes);
                break;
            default:
                break;
        }

    }

    @Override
    public String getConsumerId() {
        return "join_reward_activity_message";
    }

    private void signIn(List<Object> list){
        String userId = list.get(0).toString();
        Date signInDate = (Date)list.get(1);
        Long beanNum = (Long)list.get(2);
        Long recordId = (Long)list.get(3);
        UserOperationRecord userOperationRecord = new UserOperationRecord();
        userOperationRecord.setObjectId(userId);
        userOperationRecord.setUserType(UserType.USER);
        userOperationRecord.setOperationType(OperationType.SIGN_IN);
        userOperationRecord.setOperation(Operation.ADD);
        userOperationRecord.setOperationNum(beanNum);
        userOperationRecord.setAssetType(AssetType.BEAN);
        userOperationRecord.setBeanAmount(beanNum);
        userOperationRecord.setCompleteCount(1l);
        userOperationRecord.setCompleteTime(signInDate);
        userOperationRecord.setDrawBeanAmount(beanNum);
        userOperationRecord.setDrawCount(1l);
        userOperationRecord.setBusinessId(recordId.toString());
        userOperationRecord.setUserTaskStatus(UserTaskStatus.ONGOING);
        userOperationRecord.setOriginUserId(userId);
        userTaskOperationService.saveUserOperation(userOperationRecord);
    }

    private void updateUserData(List<Object> list){
        Long userId = (Long)list.get(0);
        TaskSetting taskSetting = userTaskOperationService.getTaskSetting(TaskType.COMPLETE_USER_DATA_V2);
        if(taskSetting == null || taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)){
            return;
        }
        String key = "UPDATE_USER_DATA_LOCK_KEY_"+userId;
        RedisLock redisLock = new RedisLock(stringRedisTemplate, key, 5, TimeUnit.SECONDS);
        try {
            if (!redisLock.tryLock()) {
                throw new RedisLockFailException();
            }
            UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userId);
            if (userInfoIndex == null) {
                logger.error("用户信息未找到, userId = {}", userId);
                return;
            }

            UserTaskAccount userTaskAccount = userTaskAccountApiService.findUserTaskAccount(userInfoIndex.getPhone(), TaskType.COMPLETE_USER_DATA_V2);
            if (userTaskAccount != null && userTaskAccount.getStatus().equals(UserTaskStatus.FINISHED)) {
                logger.error("用户完善资料任务已经完成, userId = {}", userId);
                return;
            }

            String nickname = userInfoIndex.getNickName();
            //判断是否修改过昵称
            boolean updNickname = Pattern.matches("^半径[0-9]{6,8}", nickname) || Pattern.matches("^友邻[0-9]{6}", nickname);
            //判断是否有头像信息
            boolean updIconurl = StringUtils.isEmpty(userInfoIndex.getIconUrl());
            //判断性别
            boolean updSexType = userInfoIndex.getSex().equals(SexType.Unknown);
            //判断是否有年龄段
            boolean updAgeGroup = StringUtils.isEmpty(userInfoIndex.getAgeGroup());
            if (updNickname || updIconurl || updSexType || updAgeGroup) {
                logger.error("信息不完整, userId = {}", userId);
                return;
            }
            Long incomeNum = getUserIncomeByUserId(userId, taskSetting);
            if (taskSetting.getMaxTimes() != -1 && incomeNum >= taskSetting.getMaxTimes()) {
                return;
            }
            if (taskSetting.getLimitUnit().equals(LimitUnit.TIMES)) {
                incomeNum++;
            } else {
                incomeNum += taskSetting.getRewardAmount();
            }
            this.saveUserOperation(userId, OperationType.COMPLETE_USER_DATA_V2, taskSetting.getRewardAmount(), false,
                    taskSetting, incomeNum,userId.toString());
        }finally {
            redisLock.unlock();
        }
    }

    private void openDoor(List<Object> list){
        String phone = (String)list.get(0);
        Long communityExtId = (Long)list.get(1);
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByPhone(phone);
        Long userId = userInfoIndex.getId();
        TaskSetting taskSetting = userTaskOperationService.getTaskSetting(TaskType.FIRST_OPEN_DOOR_V2);
        if(taskSetting == null || taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)){
            return;
        }
        String key = "OPEN_DOOR_LOCK_KEY_"+userId;
        RedisLock redisLock = new RedisLock(stringRedisTemplate, key, 5, TimeUnit.SECONDS);
        try {
            if (!redisLock.tryLock()) {
                throw new RedisLockFailException();
            }
            UserTaskAccount userTaskAccount = userTaskAccountApiService.findUserTaskAccount(userInfoIndex.getPhone(), TaskType.FIRST_OPEN_DOOR_V2);
            if (userTaskAccount != null && userTaskAccount.getStatus().equals(UserTaskStatus.FINISHED)) {
                logger.error("用户首次开门任务已经完成, userId = {}", userId);
                return;
            }
            Long incomeNum = getUserIncomeByUserId(userId, taskSetting);
            if (taskSetting.getMaxTimes() != -1 && incomeNum >= taskSetting.getMaxTimes()) {
                return;
            }
            if (taskSetting.getLimitUnit().equals(LimitUnit.TIMES)) {
                incomeNum++;
            } else {
                incomeNum += taskSetting.getRewardAmount();
            }
            this.saveUserOperation(userId, OperationType.FIRST_OPEN_DOOR_V2, taskSetting.getRewardAmount(), false,
                    taskSetting, "", "", communityExtId, incomeNum,userId.toString());
        }finally {
            redisLock.unlock();
        }
    }

    private void authHouse(List<Object> list){
        Long userHouseRelationId = (Long)list.get(0);
        Long userId = (Long)list.get(1);
        TaskSetting taskSetting = userTaskOperationService.getTaskSetting(TaskType.LOGIN_AUTH_V2);
        if(taskSetting == null || taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)){
            return;
        }
        Long incomeNum = getUserIncomeByUserId(userId,taskSetting);
        if(taskSetting.getMaxTimes()!=-1 && incomeNum >= taskSetting.getMaxTimes()){
            return;
        }
        if(taskSetting.getLimitUnit().equals(LimitUnit.TIMES)){
            incomeNum ++;
        }else {
            incomeNum += taskSetting.getRewardAmount();
        }
        this.saveUserOperation(userId,OperationType.LOGIN_AUTH_V2,taskSetting.getRewardAmount(),false,
                taskSetting,userHouseRelationId.toString(),incomeNum,userId.toString());
    }

    private void postComment(List<Object> list){
        String commentId = (String)list.get(0);
        String commentUserId = (String)list.get(1);
        logger.error("commentId:" + commentId + ",commentUserId:" + commentUserId);
        CommentInfoIndex commentInfoIndex = commentInfoIndexApiService.get(commentId);
        String postId = commentInfoIndex.getPostId();
        PostInfoIndex postInfoIndex = postInfoIndexApiService.get(postId);
        if(postInfoIndex.getUserType().equals(cn.lehome.bean.content.entity.enums.post.UserType.ROBOT)){
            return;
        }
        Long userId = postInfoIndex.getUserId();
        String key = "POST_USER_LOCK_KEY_"+userId;
        RedisLock redisLock = new RedisLock(stringRedisTemplate, key, 5, TimeUnit.SECONDS);
        try {
            if (!redisLock.tryLock()) {
                throw new RedisLockFailException();
            }
            TaskSetting taskSetting = userTaskOperationService.getPostCommentReward(userId, commentUserId, postId);
            if (taskSetting == null) {
                return;
            }
            Long incomeNum = userTaskOperationService.getPostIncome(userId, taskSetting);
            if (taskSetting.getLimitUnit().equals(LimitUnit.TIMES)) {
                incomeNum++;
            } else {
                incomeNum += taskSetting.getRewardAmount();
            }
            boolean isUpdated = this.saveUserOperation(userId, OperationType.POST_COMMENT, taskSetting.getRewardAmount(), false,
                    taskSetting, postId, commentId, incomeNum,commentUserId);
            if(isUpdated) {
                userTaskOperationService.setPostCommentRewardCache(userId, commentUserId, postId, taskSetting);
            }
        }finally {
            redisLock.unlock();
        }
    }

    private void postAgree(List<Object> list){
        String postId = (String)list.get(0);
        Long agreeUserId = (Long)list.get(1);
        Long agreeId = (Long)list.get(2);
        logger.error("postId:" + postId + ",agreeUserId:" + agreeUserId);
        PostInfoIndex postInfoIndex = postInfoIndexApiService.get(postId);
        if(postInfoIndex.getUserType().equals(cn.lehome.bean.content.entity.enums.post.UserType.ROBOT)){
            return;
        }
        Long userId = postInfoIndex.getUserId();
        String key = "POST_USER_LOCK_KEY_"+userId;
        RedisLock redisLock = new RedisLock(stringRedisTemplate, key, 5, TimeUnit.SECONDS);
        try {
            if (!redisLock.tryLock()) {
                throw new RedisLockFailException();
            }
            TaskSetting taskSetting = userTaskOperationService.getPostAgreeReward(userId, agreeUserId, postId);
            if (taskSetting == null) {
                return;
            }
            Long incomeNum = userTaskOperationService.getPostIncome(userId, taskSetting);
            if (taskSetting.getLimitUnit().equals(LimitUnit.TIMES)) {
                incomeNum++;
            } else {
                incomeNum += taskSetting.getRewardAmount();
            }
            boolean isUpdated = this.saveUserOperation(userId, OperationType.POST_AGREE, taskSetting.getRewardAmount(), false,
                    taskSetting, postId, agreeId.toString(), incomeNum,agreeUserId.toString());
            if(isUpdated) {
                userTaskOperationService.setPostAgreeRewardCache(userId, agreeUserId, postId, taskSetting);
            }
        }finally{
            redisLock.unlock();
        }
    }

    /**
     * 评论获得点赞处理
     */
    private void postCommentAgree(List<Object> list){
        String commentId = (String)list.get(0);
        Long agreeUserId = (Long)list.get(1);
        Long agreeId = (Long)list.get(2);
        logger.error("commentId:" + commentId + ",agreeUserId:" + agreeUserId);
        CommentInfoIndex commentInfoIndex = commentInfoIndexApiService.get(commentId);
        // 收益人
        Long userId = commentInfoIndex.getCommentUserId();
        String key = "POST_COMMENT_AGREE_USER_LOCK_KEY_" + userId;
        RedisLock redisLock = new RedisLock(stringRedisTemplate, key, 5, TimeUnit.SECONDS);
        try {
            if (!redisLock.tryLock()) {
                throw new RedisLockFailException();
            }
            TaskSetting taskSetting = userTaskOperationService.getPostCommentAgreeReward(userId, agreeUserId, commentId);
            if (taskSetting == null) {
                return;
            }
            Long incomeNum = userTaskOperationService.getPostIncome(userId, taskSetting);
            if (taskSetting.getLimitUnit().equals(LimitUnit.TIMES)) {
                incomeNum++;
            } else {
                incomeNum += taskSetting.getRewardAmount();
            }
            boolean isUpdated = this.saveUserOperation(userId, OperationType.POST_COMMENT_AGREE, taskSetting.getRewardAmount(), false,
                    taskSetting, commentId, agreeId.toString(), incomeNum, agreeUserId.toString());
            if(isUpdated) {
                userTaskOperationService.setPostCommentAgreeRewardCache(userId, agreeUserId, commentId, taskSetting);
            }
        }finally{
            redisLock.unlock();
        }
    }

    private void postReply(List<Object> list){
        //回复的ID
        String commentId = (String)list.get(0);
        //回复人的ID
        String replyUserId = (String)list.get(1);
        logger.error("commentId:" + commentId + ",replyUserId:" + replyUserId);
        CommentInfoIndex commentInfoIndex = commentInfoIndexApiService.get(commentId);
        //评论的ID
        String beCommentId = commentInfoIndex.getBeCommentId();
        //评论
        CommentInfoIndex beCommentInfoIndex = commentInfoIndexApiService.get(beCommentId);
        if(beCommentInfoIndex.getCommentUserType().equals(cn.lehome.bean.content.entity.enums.post.UserType.ROBOT)){
            return;
        }
        //评论人
        Long userId = beCommentInfoIndex.getCommentUserId();
        String key = "POST_USER_LOCK_KEY_"+userId;
        RedisLock redisLock = new RedisLock(stringRedisTemplate, key, 5, TimeUnit.SECONDS);
        try {
            if (!redisLock.tryLock()) {
                throw new RedisLockFailException();
            }
            TaskSetting taskSetting = userTaskOperationService.getPostReplyReward(userId, replyUserId, beCommentId);
            if (taskSetting == null) {
                return;
            }
            Long incomeNum = userTaskOperationService.getPostIncome(userId, taskSetting);
            if (taskSetting.getLimitUnit().equals(LimitUnit.TIMES)) {
                incomeNum++;
            } else {
                incomeNum += taskSetting.getRewardAmount();
            }
            boolean isUpdated = this.saveUserOperation(userId, OperationType.AGREE_REPLY, taskSetting.getRewardAmount(), false,
                    taskSetting, beCommentId, commentId, incomeNum,replyUserId);
            if(isUpdated) {
                userTaskOperationService.setPostReplyRewardCache(userId, replyUserId, beCommentId, taskSetting);
            }
        }finally {
            redisLock.unlock();
        }

    }

    private void postTop(List<Object> list){
        String postId = (String)list.get(0);
        PostInfoIndex postInfoIndex = postInfoIndexApiService.get(postId);
        if(postInfoIndex.getUserType().equals(cn.lehome.bean.content.entity.enums.post.UserType.ROBOT)){
            return;
        }
        Long userId = postInfoIndex.getUserId();
        TaskSetting taskSetting = userTaskOperationService.getTaskSetting(TaskType.POST_TOP);
        if(taskSetting == null || taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)){
            return;
        }
        if(postInfoIndex.getTopStatus().equals(YesNoStatus.NO)){
            return;
        }
        Long incomeNum = getUserIncomeByUserId(userId,taskSetting);
        if(taskSetting.getMaxTimes()!=-1 && incomeNum >= taskSetting.getMaxTimes()){
            return;
        }
        if(taskSetting.getLimitUnit().equals(LimitUnit.TIMES)){
            incomeNum ++;
        }else {
            incomeNum += taskSetting.getRewardAmount();
        }
        this.saveUserOperation(userId,OperationType.POST_TOP,taskSetting.getRewardAmount(),false,
                taskSetting,postId,incomeNum,"0000");
    }


    private void postSelected(List<Object> list){
        String postId = (String)list.get(0);
        PostInfoIndex postInfoIndex = postInfoIndexApiService.get(postId);
        if(postInfoIndex.getUserType().equals(cn.lehome.bean.content.entity.enums.post.UserType.ROBOT)){
            return;
        }
        Long userId = postInfoIndex.getUserId();
        TaskSetting taskSetting = userTaskOperationService.getTaskSetting(TaskType.POST_SELECTED);
        if(taskSetting == null || taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)){
            return;
        }
        if(postInfoIndex.getIsSelected().equals(YesNoStatus.NO)){
            return;
        }
        Long incomeNum = getUserIncomeByUserId(userId,taskSetting);
        if(taskSetting.getMaxTimes()!=-1 && incomeNum >= taskSetting.getMaxTimes()){
            return;
        }
        if(taskSetting.getLimitUnit().equals(LimitUnit.TIMES)){
            incomeNum ++;
        }else {
            incomeNum += taskSetting.getRewardAmount();
        }
        this.saveUserOperation(userId,OperationType.POST_SELECTED,taskSetting.getRewardAmount(),true,
                taskSetting,postId,incomeNum,"0000");
    }

    private void postIndex(List<Object> list){
        String postId = (String)list.get(0);
        PostInfoIndex postInfoIndex = postInfoIndexApiService.get(postId);
        Long userId = postInfoIndex.getUserId();
        if(postInfoIndex.getUserType().equals(cn.lehome.bean.content.entity.enums.post.UserType.ROBOT)){
            return;
        }
        TaskSetting taskSetting = userTaskOperationService.getTaskSetting(TaskType.POST_INDEX);
        if(taskSetting == null || taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)){
            return;
        }
        Long incomeNum = getUserIncomeByUserId(userId,taskSetting);
        if(taskSetting.getMaxTimes()!=-1 && incomeNum >= taskSetting.getMaxTimes()){
            return;
        }
        if(taskSetting.getLimitUnit().equals(LimitUnit.TIMES)){
            incomeNum ++;
        }else {
            incomeNum += taskSetting.getRewardAmount();
        }
        this.saveUserOperation(userId,OperationType.POST_INDEX,taskSetting.getRewardAmount(),false,
                taskSetting,postId,incomeNum,"0000");
    }

    private void contribution(List<Object> list){
        Long userId = (Long)list.get(0);
        Long apprenticeId = (Long)list.get(1);
        Long operationNum = (Long)list.get(2);
        TaskSetting taskSetting = userTaskOperationService.getTaskSetting(TaskType.SUBORDINATE_CONTRIBUTION);
        if(taskSetting == null || taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)){
            return;
        }
        Long incomeNum = getUserIncomeByUserId(userId,taskSetting);
        if(taskSetting.getMaxTimes()!=-1 && incomeNum >= taskSetting.getMaxTimes()){
            return;
        }
        if(taskSetting.getLimitUnit().equals(LimitUnit.TIMES)){
            incomeNum ++;
        }else {
            incomeNum += operationNum;
        }
        this.saveUserOperation(userId,OperationType.SUBORDINATE_CONTRIBUTION,operationNum,true,
                taskSetting,incomeNum,apprenticeId.toString());

    }

    private void inviteFriend(List<Object> list){
        Long userId = (Long)list.get(0);
        Long apprenticeId = (Long)list.get(1);
        Long operationNum = (Long)list.get(2);
        TaskSetting taskSetting = userTaskOperationService.getTaskSetting(TaskType.INVITE_FRIEND_V2);
        if(taskSetting == null /*|| taskSetting.getEnabledStatus().equals(EnableDisableStatus.DISABLE)*/){
            return;
        }
        Long incomeNum = getUserIncomeByUserId(userId,taskSetting);
        if(taskSetting.getMaxTimes()!=-1 && incomeNum >= taskSetting.getMaxTimes()){
            return;
        }
        if(taskSetting.getLimitUnit().equals(LimitUnit.TIMES)){
            incomeNum ++;
        }else {
            incomeNum += operationNum;
        }

        this.saveUserOperation(userId,OperationType.INVITE_FRIEND_V2,operationNum,true,
                taskSetting,incomeNum,apprenticeId.toString());
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userId);
        Long reward = getInviteFriendReward(userId);
        String message = userInfoIndex.getNickName() + "邀请了好友，获得" + reward/100 + "元现金";
        this.addCacheMessage(message);
    }
    private void addCacheMessage(String message){
        ListOperations<String,String> listOperations = stringRedisTemplate.opsForList();
        String prefix = message.split("，")[0];
        boolean isCover = false;
        long size = listOperations.size(PubConstant.SCROLL_MESSAGE_KEY);
        for(int i = 0;i<size;i++){
            String m = listOperations.rightPop(PubConstant.SCROLL_MESSAGE_KEY);
            if(m.startsWith(prefix)){
                isCover = true;
                continue;
            }
            if(listOperations.size(PubConstant.SCROLL_MESSAGE_KEY)<10) {
                listOperations.leftPush(PubConstant.SCROLL_MESSAGE_KEY, m);
            }
        }
        if(!isCover){
            listOperations.rightPop(PubConstant.SCROLL_MESSAGE_KEY);
        }
        logger.info("leftPush:" + message);
        listOperations.leftPush(PubConstant.SCROLL_MESSAGE_KEY, message);

    }
    private void saveUserOperation(Long userId,
                                   OperationType operationType,
                                   Long operationNum,
                                   boolean isAutoDrew,
                                   TaskSetting taskSetting,
                                   Long incomeNum,
                                   String originUserId){
        this.saveUserOperation(userId,operationType,operationNum,isAutoDrew,taskSetting,"","",null,incomeNum,originUserId);
    }

    private boolean saveUserOperation(Long userId,
                                   OperationType operationType,
                                   Long operationNum,
                                   boolean isAutoDrew,
                                   TaskSetting taskSetting,
                                   String businessId,
                                   Long incomeNum,
                                   String originUserId){
        return this.saveUserOperation(userId,operationType,operationNum,isAutoDrew,taskSetting,businessId,"",null,incomeNum,originUserId);
    }

    private boolean saveUserOperation(Long userId,
                                   OperationType operationType,
                                   Long operationNum,
                                   boolean isAutoDrew,
                                   TaskSetting taskSetting,
                                   String businessId,
                                   String originId,
                                   Long incomeNum,
                                   String originUserId){
        return this.saveUserOperation(userId,operationType,operationNum,isAutoDrew,taskSetting,businessId,originId,null,incomeNum,originUserId);
    }

    private boolean saveUserOperation(Long userId,
                                   OperationType operationType,
                                   Long operationNum,
                                   boolean isAutoDrew,
                                   TaskSetting taskSetting,
                                   String businessId,
                                   String originId,
                                   Long communityExtId,
                                   Long incomeNum,
                                   String originUserId){
        logger.info("userId:" + userId + " taskSetting:" + taskSetting.getType().getName());
        if(!isUpdateVersion(userId)){
            logger.info("当前用户【" + userId + "】app为老版本，不能进行奖励操作");
            return false;
        }
        UserOperationRecord userOperationRecord = new UserOperationRecord();
        userOperationRecord.setObjectId(userId.toString());
        userOperationRecord.setUserType(UserType.USER);
        userOperationRecord.setOperationType(operationType);
        userOperationRecord.setOperation(Operation.ADD);
        userOperationRecord.setOperationNum(operationNum);
        userOperationRecord.setAssetType(taskSetting.getAssetType());
        userOperationRecord.setBusinessId(businessId);
        userOperationRecord.setOriginId(originId);
        userOperationRecord.setCompleteCount(1l);
        userOperationRecord.setCommunityId(communityExtId);
        userOperationRecord.setCompleteTime(new Date());
        userOperationRecord.setOriginUserId(originUserId);
        if(taskSetting.getAssetType().equals(AssetType.BEAN)){
            userOperationRecord.setBeanAmount(operationNum);
        }else{
            userOperationRecord.setAmount(operationNum);
        }
        //设置领取状态
        if(isAutoDrew){
            userOperationRecord.setDrewStatus(TaskDrewStatus.CANNOT_DREW);
            userOperationRecord.setDrawCount(1l);
            if(taskSetting.getAssetType().equals(AssetType.BEAN)) {
                userOperationRecord.setDrawBeanAmount(operationNum);
            }else{
                userOperationRecord.setDrawAmount(operationNum);
            }
        }else{
            userOperationRecord.setDrewStatus(TaskDrewStatus.UNCLAIMED);
        }
        //设置任务状态
        if(taskSetting.getLimitType().equals(LimitType.ONECE)){
            //如果是一次性任务
            userOperationRecord.setUserTaskStatus(UserTaskStatus.FINISHED);
        }else if(taskSetting.getLimitType().equals(LimitType.DAILY)){
            //如果是每日任务
            userOperationRecord.setUserTaskStatus(UserTaskStatus.ONGOING);
        }else{
            //如果是连续任务
            if(taskSetting.getMaxTimes().equals(-1)){
                //如果次数没有限制
                userOperationRecord.setUserTaskStatus(UserTaskStatus.ONGOING);
            }else{
                if(taskSetting.getMaxTimes() > incomeNum){
                    userOperationRecord.setUserTaskStatus(UserTaskStatus.ONGOING);
                }else{
                    userOperationRecord.setUserTaskStatus(UserTaskStatus.FINISHED);
                }
            }
        }
        userTaskOperationService.saveUserOperation(userOperationRecord);
        return true;
    }

    private Long getUserIncomeByUserId(Long userId,TaskSetting taskSetting){
        String phone = this.getUserPhone(userId);
        return getUserIncomeByPhone(phone,taskSetting);
    }

    private Long getUserIncomeByPhone(String phone,TaskSetting taskSetting){
        UserTaskAccount userTaskAccount = userTaskAccountApiService.findUserTaskAccount(phone,taskSetting.getType());
        if(userTaskAccount == null){
            return 0l;
        }
        if(taskSetting.getLimitUnit().equals(LimitUnit.BEAN)){
            return userTaskAccount.getBeanAmount();
        }else if(taskSetting.getLimitUnit().equals(LimitUnit.MONEY)){
            return userTaskAccount.getAmount();
        }else{
            return userTaskAccount.getCompleteCount();
        }
    }

    private Long getInviteFriendReward(Long userId){
        String phone = this.getUserPhone(userId);
        UserTaskAccount userTaskAccount = userTaskAccountApiService.findUserTaskAccount(phone,TaskType.INVITE_FRIEND_V2);
        if(userTaskAccount == null){
            return 0l;
        }
        return userTaskAccount.getAmount();
    }

    private String getUserPhone(Long userId){
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userId);
        return userInfoIndex.getPhone();
    }

    private boolean isUpdateVersion(Long userId){
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userId);
        String clientId = userInfoIndex.getClientId();
        if (StringUtils.isBlank(clientId)) {
            clientId = userInfoIndex.getLastClientId();
        }
        ClientDeviceIndex clientDeviceIndex = clientDeviceIndexApiService.get(clientId, ClientType.SQBJ);
        if(ClientOSType.IOS.equals(clientDeviceIndex.getClientOSType())){
            return IOS_MIN_VERSION_CODE <= clientDeviceIndex.getAppVersionCode();
        }else if(ClientOSType.ANDROID.equals(clientDeviceIndex.getClientOSType())){
            return ANDROID_MIN_VERSION_CODE <= clientDeviceIndex.getAppVersionCode();
        }
        return false;
    }
}
