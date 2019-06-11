package cn.lehome.dispatcher.queue.service.task;

import cn.lehome.base.api.business.activity.bean.task.TaskSetting;
import cn.lehome.bean.business.activity.enums.task.TaskType;
import cn.lehome.dispatcher.queue.bean.UserOperationRecord;

/**
 * Created by zuoguodong on 2018/5/16
 */
public interface UserTaskOperationService {

    void saveUserOperation(UserOperationRecord userOperationRecord);

    TaskSetting getPostCommentReward(Long userId,String commentUserId,String postId);

    void setPostCommentRewardCache(Long userId,String commentUserId,String postId,TaskSetting taskSetting);

    TaskSetting getPostAgreeReward(Long userId,Long agreeUserId,String postId);

    TaskSetting getPostCommentAgreeReward(Long userId, Long agreeUserId, String commentId);

    void setPostAgreeRewardCache(Long userId,Long agreeUserId,String postId,TaskSetting taskSetting);

    void setPostCommentAgreeRewardCache(Long userId,Long agreeUserId, String commentId, TaskSetting taskSetting);

    TaskSetting getPostReplyReward(Long userId,String replyUserId,String beCommentId);

    void setPostReplyRewardCache(Long userId,String replyUserId,String beCommentId,TaskSetting taskSetting);

    TaskSetting getTaskSetting(TaskType taskType);

    Long getPostIncome(Long userId,TaskSetting taskSetting);

}
