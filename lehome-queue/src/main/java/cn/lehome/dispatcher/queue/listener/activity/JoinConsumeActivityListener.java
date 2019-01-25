package cn.lehome.dispatcher.queue.listener.activity;


import cn.lehome.base.api.advertising.bean.task.BeanConsume;
import cn.lehome.base.api.advertising.bean.task.TaskSetting;
import cn.lehome.base.api.advertising.constant.JoinConsumeActivityTypeConstants;
import cn.lehome.base.api.advertising.service.task.BeanConsumeSettingApiService;
import cn.lehome.base.api.advertising.service.task.UserTaskOperationRecordApiService;
import cn.lehome.base.api.content.bean.post.PostInfoIndex;
import cn.lehome.base.api.content.service.post.PostInfoIndexApiService;
import cn.lehome.base.api.tool.event.JoinActivityEventBean;
import cn.lehome.bean.advertising.enums.task.*;
import cn.lehome.dispatcher.queue.bean.UserOperationRecord;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.task.UserTaskOperationService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import cn.lehome.framework.bean.core.enums.Operation;
import cn.lehome.framework.bean.core.enums.OperationType;
import cn.lehome.framework.bean.core.enums.UserType;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.List;

/**
 * Created by zuoguodong on 2018/5/16.
 */
public class JoinConsumeActivityListener extends AbstractJobListener {

    @Autowired
    PostInfoIndexApiService postInfoIndexApiService;

    @Autowired
    UserTaskOperationService userTaskOperationService;

    @Autowired
    UserTaskOperationRecordApiService userTaskOperationRecordApiService;

    @Autowired
    BeanConsumeSettingApiService beanConsumeSettingApiService;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        SimpleEventMessage<JoinActivityEventBean> simpleEventMessage = (SimpleEventMessage<JoinActivityEventBean>) eventMessage;
        JoinActivityEventBean joinActivityEventBean = simpleEventMessage.getData();
        List<Object> attributes = joinActivityEventBean.getAttributes();
        switch (joinActivityEventBean.getJoinActivityType()) {
            case JoinConsumeActivityTypeConstants.DEL_POST_MANAGER:
                logger.info("管理员删贴");
                delPostByManager(attributes);
                break;
            case JoinConsumeActivityTypeConstants.DEL_POST_SELF:
                logger.info("自己删贴");
                delPostBySelf(attributes);
                break;
            case JoinConsumeActivityTypeConstants.DRAW_CARD:
                logger.info("抽卡");
                drawCard(attributes);
                break;
            case JoinConsumeActivityTypeConstants.STEAL_CARD:
                logger.info("偷卡");
                stealCard(attributes);
                break;
            default:
                break;
        }

    }

    @Override
    public String getConsumerId() {
        return "join_consume_activity_message";
    }

    private void delPostByManager(List<Object> list){
        List<String> postIdList = (List<String>)list.get(0);
        postIdList.forEach(postId -> {
            PostInfoIndex postInfoIndex = postInfoIndexApiService.get(postId);
            Long userId = postInfoIndex.getUserId();
            Long beanSum = userTaskOperationRecordApiService.findBeanSumByPostId(postId);
            BeanConsume beanConsume = beanConsumeSettingApiService.findByConsumeType(ConsumeType.DEL_POST_MANAGER);
            beanSum += beanConsume.getBeanNum();
            UserOperationRecord userOperationRecord = new UserOperationRecord();
            userOperationRecord.setObjectId(userId.toString());
            userOperationRecord.setUserType(UserType.USER);
            userOperationRecord.setOperationType(OperationType.DEL_POST_MANAGER);
            userOperationRecord.setOperation(Operation.SUB);
            userOperationRecord.setOperationNum(beanSum);
            //只扣除豆
            userOperationRecord.setAssetType(AssetType.BEAN);
            userOperationRecord.setBusinessId(postId);
            userOperationRecord.setOriginUserId("0000");
            userTaskOperationService.saveUserOperation(userOperationRecord);
        });
    }

    private void delPostBySelf(List<Object> list){
        String postId = (String)list.get(0);
        PostInfoIndex postInfoIndex = postInfoIndexApiService.get(postId);
        Long userId = postInfoIndex.getUserId();
        Long beanSum = userTaskOperationRecordApiService.findBeanSumByPostId(postId);
        if(beanSum==0){
            return;
        }
        UserOperationRecord userOperationRecord = new UserOperationRecord();
        userOperationRecord.setObjectId(userId.toString());
        userOperationRecord.setUserType(UserType.USER);
        userOperationRecord.setOperationType(OperationType.DEL_POST_SELF);
        userOperationRecord.setOperation(Operation.SUB);
        userOperationRecord.setOperationNum(beanSum);
        //只扣除豆
        userOperationRecord.setAssetType(AssetType.BEAN);
        userOperationRecord.setBusinessId(postId);
        userOperationRecord.setOriginUserId(userId.toString());
        userTaskOperationService.saveUserOperation(userOperationRecord);
    }

    private void drawCard(List<Object> list){
        Long userId = (Long)list.get(0);
        Long businessId = (Long)list.get(1);
        BeanConsume beanConsume = beanConsumeSettingApiService.findByConsumeType(ConsumeType.DRAW_CARD);
        UserOperationRecord userOperationRecord = new UserOperationRecord();
        userOperationRecord.setObjectId(userId.toString());
        userOperationRecord.setUserType(UserType.USER);
        userOperationRecord.setOperationType(OperationType.DRAW_CARD);
        userOperationRecord.setOperation(Operation.SUB);
        userOperationRecord.setOperationNum(Long.valueOf(beanConsume.getBeanNum()));
        userOperationRecord.setAssetType(AssetType.BEAN);
        userOperationRecord.setBusinessId(businessId.toString());
        userOperationRecord.setOriginUserId(userId.toString());
        userTaskOperationService.saveUserOperation(userOperationRecord);
    }

    private void stealCard(List<Object> list){
        Long userId = (Long)list.get(0);
        Long businessId = (Long)list.get(1);
        BeanConsume beanConsume = beanConsumeSettingApiService.findByConsumeType(ConsumeType.STEAL_CARD);
        UserOperationRecord userOperationRecord = new UserOperationRecord();
        userOperationRecord.setObjectId(userId.toString());
        userOperationRecord.setUserType(UserType.USER);
        userOperationRecord.setOperationType(OperationType.STEAL_CARD);
        userOperationRecord.setOperation(Operation.SUB);
        userOperationRecord.setOperationNum(Long.valueOf(beanConsume.getBeanNum()));
        userOperationRecord.setAssetType(AssetType.BEAN);
        userOperationRecord.setBusinessId(businessId.toString());
        userOperationRecord.setOriginUserId(userId.toString());
        userTaskOperationService.saveUserOperation(userOperationRecord);
    }

}
