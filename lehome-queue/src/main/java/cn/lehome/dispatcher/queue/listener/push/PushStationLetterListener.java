package cn.lehome.dispatcher.queue.listener.push;

import cn.lehome.base.api.common.operation.bean.push.PushPlan;
import cn.lehome.base.api.common.operation.bean.push.StationLetterInfo;
import cn.lehome.base.api.common.operation.service.push.PushPlanApiService;
import cn.lehome.base.api.common.operation.service.push.StationLetterApiService;
import cn.lehome.base.api.user.bean.message.UserMessage;
import cn.lehome.base.api.user.bean.message.UserMessageIndex;
import cn.lehome.base.api.user.bean.user.QUserInfoIndex;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.message.UserMessageApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.dispatcher.queue.listener.AbstractSessionJobListener;
import cn.lehome.dispatcher.queue.service.push.PushService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.exception.sql.NotFoundRecordException;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.bean.core.enums.PushGroupType;
import cn.lehome.framework.bean.core.enums.SendStatus;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by zhanghuan on 2018/7/12.
 */
public class PushStationLetterListener extends AbstractSessionJobListener {

    @Autowired
    private PushPlanApiService pushPlanApiService;

    @Autowired
    private StationLetterApiService stationLetterApiService;

    @Autowired
    private PushService pushService;

    @Autowired
    private UserMessageApiService userMessageApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    public static final Integer MAX_SEND_SIZE=1000;
    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        logger.error("=======进入站内信消息队列处理逻辑.....");
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        PushPlan pushPlan = pushPlanApiService.findOne(longEventMessage.getData());
        if (pushPlan == null){
            logger.error("推送站内信任务未找到, id = {}", longEventMessage.getData());
            return;
        }
        pushPlan.setSendStatus(SendStatus.DEALING);
        pushPlan = pushPlanApiService.update(pushPlan);
        boolean isDeal = true;
        try {
            Long objectId = pushPlan.getObjectId();
            logger.error("objectId : "+objectId);
            StationLetterInfo stationLetterInfo = stationLetterApiService.get(objectId);
            logger.error("stationLetterInfo: "+stationLetterInfo.getStationLetterId());
            List<Long> allUserId = null;
            List<UserMessageIndex> userMessageList = null;
            Integer count = 0;
            if (StringUtils.isNotEmpty(pushPlan.getOssUrl())){
                logger.error("开始获取Excel数据并保存...");
                Set<String> userPhoneSet = pushService.readExcelUserInfoMobiles(pushPlan.getOssUrl());
                List<UserInfoIndex> userInfoIndexList = Lists.newArrayList();
                if (!CollectionUtils.isEmpty(userPhoneSet)){
                    if (userPhoneSet.size()>MAX_SEND_SIZE){
                        List<List<String>> subUserInfos = Lists.partition(Lists.newArrayList(userPhoneSet),MAX_SEND_SIZE);
                        for (List<String> userPhones : subUserInfos){
                            List<UserInfoIndex> userInfoIndices = userInfoIndexApiService.findAll(ApiRequest.newInstance().filterIn(QUserInfoIndex.phone,userPhones));
                            userInfoIndexList.addAll(userInfoIndices);
                        }
                    }else {
                        userInfoIndexList = userInfoIndexApiService.findAll(ApiRequest.newInstance().filterIn(QUserInfoIndex.phone,userPhoneSet));
                    }
                }
                allUserId = userInfoIndexList.stream().map(UserInfoIndex::getId).collect(Collectors.toList());
                userMessageList = userMessageApiService.saveBatch(fillUserMessageInfo(allUserId,stationLetterInfo,pushPlan.getId()));
                count = userMessageList.size();
            }else{
                logger.error("进入根据区域或范围获取所有用户信息...");
                Map<String,List<UserInfoIndex>> userInfoMaps = pushService.findAllUserInfo(pushPlan);
                List<UserInfoIndex> userInfoIndexList =userInfoMaps.get("commonUserInfos");
                if (CollectionUtils.isEmpty(userInfoIndexList)){
                    throw new NotFoundRecordException("用户信息未找到!");
                }
                allUserId = userInfoIndexList.stream().distinct().map(UserInfoIndex::getId).collect(Collectors.toList());
                logger.error("查找到用户: "+allUserId.size()+"个");
                if (allUserId.size()>MAX_SEND_SIZE){
                    List<List<Long>> subList = Lists.partition(allUserId,MAX_SEND_SIZE);
                    for (List<Long> list : subList){
                        userMessageList = userMessageApiService.saveBatch(fillUserMessageInfo(list,stationLetterInfo,pushPlan.getId()));
                        count+=userMessageList.size();
                    }
                }else {
                    userMessageList = userMessageApiService.saveBatch(fillUserMessageInfo(allUserId,stationLetterInfo,pushPlan.getId()));
                    count = userMessageList.size();
                }
            }
            logger.error("站内信逻辑处理完成,共发送:"+count+"条数据");
            if (count>0){
                stationLetterInfo.setSendNumber(count);
                stationLetterApiService.update(stationLetterInfo);
            }

        }catch (Exception e){
            logger.error("推送站内信信息失败", e.getMessage());
            isDeal = false;
        }finally {
            if(isDeal){
                pushPlan.setSendStatus(SendStatus.DEAL);
                pushPlanApiService.update(pushPlan);
            }else {
                pushPlan.setSendStatus(SendStatus.DEAL_FAILED);
                pushPlanApiService.update(pushPlan);
            }
        }
    }

    private List<UserMessage> fillUserMessageInfo(List<Long> userIds,StationLetterInfo stationLetterInfo,Long planId) {
        List<UserMessage> userMessageList = Lists.newArrayList();
        logger.error("开始保存usermessage信息");
        for (Long userId : userIds){
            UserMessage userMessage = new UserMessage();
            userMessage.setContent("");
            userMessage.setPushGroupType(PushGroupType.ACTIVITY);
            userMessage.setPushPlanId(planId);
            userMessage.setUserId(userId);
            userMessage.setStationLetterId(stationLetterInfo.getStationLetterId());
            userMessageList.add(userMessage);
        }
        return userMessageList;
    }

    @Override
    public String getConsumerId() {
        return "STATION_LETTER";
    }
}
