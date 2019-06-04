package cn.lehome.dispatcher.queue.listener.push;

import cn.lehome.base.api.common.bean.device.ClientDeviceIndex;
import cn.lehome.base.api.common.bean.device.QClientDeviceIndex;
import cn.lehome.base.api.common.bean.message.MessageTemplate;
import cn.lehome.base.api.common.bean.push.PushSendRecord;
import cn.lehome.base.api.common.component.message.push.PushComponent;
import cn.lehome.base.api.common.operation.bean.push.PushPlan;
import cn.lehome.base.api.common.operation.bean.push.PushTemplateProperty;
import cn.lehome.base.api.common.operation.bean.push.StationLetterInfo;
import cn.lehome.base.api.common.operation.service.push.PushPlanApiService;
import cn.lehome.base.api.common.operation.service.push.PushTemplatePropertyApiService;
import cn.lehome.base.api.common.operation.service.push.StationLetterApiService;
import cn.lehome.base.api.common.service.device.ClientDeviceIndexApiService;
import cn.lehome.base.api.common.service.job.ScheduleJobApiService;
import cn.lehome.base.api.common.service.message.MessageTemplateApiService;
import cn.lehome.base.api.user.bean.message.UserMessage;
import cn.lehome.base.api.user.bean.user.QUserInfoIndex;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.message.UserMessageApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.common.enums.message.MessageType;
import cn.lehome.dispatcher.queue.bean.TargetValueBean;
import cn.lehome.dispatcher.queue.listener.AbstractSessionJobListener;
import cn.lehome.dispatcher.queue.service.push.PushService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.bean.core.enums.*;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.stream.Collectors;


public class PushListener extends AbstractSessionJobListener {

    @Autowired
    private PushPlanApiService pushPlanApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private PushTemplatePropertyApiService pushTemplatePropertyApiService;

    @Autowired
    private StationLetterApiService stationLetterApiService;

    @Autowired
    private ClientDeviceIndexApiService clientDeviceIndexApiService;

    @Autowired
    private PushComponent pushComponent;

    @Autowired
    private PushService pushService;

    @Autowired
    private UserMessageApiService userMessageApiService;

    @Autowired
    private ScheduleJobApiService scheduleJobApiService;

    @Autowired
    private MessageTemplateApiService messageTemplateApiService;


    private static final Integer PUSH_MAX_SIZE = 500;

    public static final Integer ES_MAX_SIZE=1024;




    @Override
    public void execute(IEventMessage eventMessage) throws Exception {

        if (!(eventMessage instanceof LongEventMessage)){
            logger.error("消息类型不对");
            return;
        }

        LongEventMessage longEventMessage = (LongEventMessage)eventMessage;
        PushPlan pushPlan = pushPlanApiService.findOne(longEventMessage.getData());

        if (pushPlan == null){
            logger.error("推送任务未找到",longEventMessage.getData());
            return;
        }
        logger.error("推送任务: {}", longEventMessage.getData());
        pushPlan.setSendStatus(SendStatus.DEALING);
        pushPlan = pushPlanApiService.update(pushPlan);
        PushTemplateProperty pushTemplateProperty = pushTemplatePropertyApiService.findOne(pushPlan.getObjectId());
        pushPlan.setPushTemplateProperty(pushTemplateProperty);
        List<Long> allUser = new ArrayList<>();
        Boolean isDeal = true;
        try{
            Set<String> vendorClientId = new HashSet<>();

            if (StringUtils.isNotEmpty(pushPlan.getOssUrl())){

                List<UserInfoIndex> userInfoIndexList = this.getOssUser(pushPlan);

                if (CollectionUtils.isNotEmpty(userInfoIndexList)){
                    this.saveUserMessage(pushPlan, userInfoIndexList);

                    vendorClientId = getVendor(userInfoIndexList, pushPlan);
                    if (CollectionUtils.isNotEmpty(vendorClientId)){
                        this.push(Lists.newArrayList(vendorClientId),pushPlan);
                    }
                }
            }else{
                Map<String,List<UserInfoIndex>> allUserData =pushService.findAllUserInfo(pushPlan);
                List<UserInfoIndex> userInfoIndexList = allUserData.get("pushUserInfos");
                List<UserInfoIndex> commonUserInfos =  allUserData.get("commonUserInfos");
                if (CollectionUtils.isNotEmpty(commonUserInfos)){
                    this.saveUserMessage(pushPlan,commonUserInfos);
                }
                if (CollectionUtils.isNotEmpty(userInfoIndexList)){
                    vendorClientId = getVendor(userInfoIndexList, pushPlan);
                    if (CollectionUtils.isNotEmpty(vendorClientId)){
                        this.push(Lists.newArrayList(vendorClientId),pushPlan);
                    }
                }
            }
        }catch (Exception e){
            isDeal = false;
            logger.error("推送失败",e);
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

    @Override
    public String getConsumerId() {
        return "push";
    }


    private PushSendRecord batchPush(List<String> vendorClientId, PushPlan pushPlan) {

        if (CollectionUtils.isNotEmpty(vendorClientId)){
            Map<String, String> params = Maps.newHashMap();
            List<TargetValueBean> param = JSON.parseArray(pushPlan.getContent(), TargetValueBean.class);
            for (TargetValueBean bean : param){
                params.put(bean.getKey(),bean.getValue());
            }
            Map<String,String> forwardParam = Maps.newHashMap();
            List<TargetValueBean> targetValueBeans = JSON.parseArray(pushPlan.getPushTemplateProperty().getTargetValue(), TargetValueBean.class);
            for (TargetValueBean bean : targetValueBeans){
                forwardParam.put(bean.getKey(),bean.getValue());
            }

            try {
                logger.error("开始执行推送");
                MessageTemplate messageTemplate = messageTemplateApiService.get(pushPlan.getPushTemplateProperty().getTemplateId());
                PushSendRecord pushSendRecord;
                if (messageTemplate.getType().equals(MessageType.SQBJ_SYSTEM_PUSH_SILENT)){
                    String templateKey = messageTemplateApiService.findTemplateKey(pushPlan.getPushTemplateProperty().getTemplateId());
                    pushSendRecord = pushComponent.pushSilent(vendorClientId.get(0),templateKey,params, forwardParam, PushOsType.ALL);
                }else{
                    pushSendRecord = pushComponent.pushBatch(vendorClientId, pushPlan.getPushTemplateProperty().getTemplateId(), params, forwardParam, PushOsType.ALL, pushPlan.getId());
                }
                logger.error("推送记录：" + JSON.toJSONString(pushSendRecord));
                return pushSendRecord;

            } catch (Exception e) {
                logger.error("本次推送失败", e);
                return null;
            }
        }
        return null;
    }

    private Set<String> getVendorClientId(List<UserInfoIndex> user, PushPlan pushPlan){
        logger.error("获取vendorClientId appVersionCode:"+pushPlan.getAppVersionCode());

        if (CollectionUtils.isNotEmpty(user)){
            Set<String> clientIds = user.stream().filter(userInfoIndex -> StringUtils.isNotEmpty(userInfoIndex.getClientId())).map(UserInfoIndex::getClientId).collect(Collectors.toSet());
            if (CollectionUtils.isNotEmpty(clientIds)){
                List<ClientDeviceIndex> clientDeviceIndexList = clientDeviceIndexApiService.findAll(ApiRequest.newInstance().filterIn(QClientDeviceIndex.clientId,clientIds).filterLike(QClientDeviceIndex.clientType,ClientType.SQBJ));
                if (CollectionUtils.isNotEmpty(clientDeviceIndexList)){
                    Set<String> vendorClientIdList;
                    if (PushOsType.ANDROID.equals(pushPlan.getPushOsType())){
                        if (pushPlan.getAppVersionCode()==0L){
                            vendorClientIdList = clientDeviceIndexList.stream().filter(clientDeviceIndex -> StringUtils.isNotEmpty(clientDeviceIndex.getVendorClientId())).filter(clientDeviceIndex -> clientDeviceIndex.getClientOSType().equals(ClientOSType.ANDROID)).map(ClientDeviceIndex::getVendorClientId).collect(Collectors.toSet());
                        }else {
                            vendorClientIdList = clientDeviceIndexList.stream().filter(clientDeviceIndex -> StringUtils.isNotEmpty(clientDeviceIndex.getVendorClientId())).filter(clientDeviceIndex -> clientDeviceIndex.getClientOSType().equals(ClientOSType.ANDROID)).filter(clientDeviceIndex ->clientDeviceIndex.getAppVersionCode() >= pushPlan.getAppVersionCode()).map(ClientDeviceIndex::getVendorClientId).collect(Collectors.toSet());
                        }

                    }else if (PushOsType.IOS.equals(pushPlan.getPushOsType())){
                        if (pushPlan.getAppVersionCode() == 0L){
                            vendorClientIdList = clientDeviceIndexList.stream().filter(clientDeviceIndex -> StringUtils.isNotEmpty(clientDeviceIndex.getVendorClientId())).filter(clientDeviceIndex -> clientDeviceIndex.getClientOSType().equals(ClientOSType.IOS)).map(ClientDeviceIndex::getVendorClientId).collect(Collectors.toSet());
                        }else {
                            vendorClientIdList = clientDeviceIndexList.stream().filter(clientDeviceIndex -> StringUtils.isNotEmpty(clientDeviceIndex.getVendorClientId())).filter(clientDeviceIndex -> clientDeviceIndex.getClientOSType().equals(ClientOSType.IOS)).filter(clientDeviceIndex -> clientDeviceIndex.getAppVersionCode() >= pushPlan.getAppVersionCode()).map(ClientDeviceIndex::getVendorClientId).collect(Collectors.toSet());
                        }
                    }else{
                        if (pushPlan.getAppVersionCode() == 0L){
                            vendorClientIdList = clientDeviceIndexList.stream().filter(clientDeviceIndex -> StringUtils.isNotEmpty(clientDeviceIndex.getVendorClientId())).map(ClientDeviceIndex::getVendorClientId).collect(Collectors.toSet());
                        }else {
                            vendorClientIdList = clientDeviceIndexList.stream().filter(clientDeviceIndex -> StringUtils.isNotEmpty(clientDeviceIndex.getVendorClientId())).filter(clientDeviceIndex -> clientDeviceIndex.getAppVersionCode() >= pushPlan.getAppVersionCode()).map(ClientDeviceIndex::getVendorClientId).collect(Collectors.toSet());
                        }
                    }
                    if (CollectionUtils.isNotEmpty(vendorClientIdList)){
                        return vendorClientIdList;
                    }
                }
            }
        }
        return null;
    }



    private void push(List<String> vendorClientId,PushPlan pushPlan){
        List<Long> pushSendIds = Lists.newArrayList();
        if (vendorClientId.size() > PUSH_MAX_SIZE){
            List<List<String>> partition = Lists.partition(vendorClientId, PUSH_MAX_SIZE);
            for (List list : partition){
                this.batchPush(list,pushPlan);
            }
        }else{
            this.batchPush(vendorClientId,pushPlan);
        }

    }

    private void saveUserMessage(PushPlan pushPlan,List<UserInfoIndex> userInfoIndexList){
        logger.error("开始保存userMessage");
        if (YesNoStatus.YES.equals(pushPlan.getPushTemplateProperty().getIsContainsStationLetter())){
            StationLetterInfo stationLetterInfo = stationLetterApiService.get(pushPlan.getPushTemplateProperty().getStationLetterId());
            List<Long> userIdList = userInfoIndexList.stream().distinct().map(UserInfoIndex::getId).collect(Collectors.toList());
            if (userIdList.size()>ES_MAX_SIZE){
                List<List<Long>> subList = Lists.partition(userIdList,ES_MAX_SIZE);
                for (List<Long> list :subList){
                    userMessageApiService.saveBatch(fillUserMessageInfo(list,stationLetterInfo,pushPlan.getId()));
                }
            }else {
                userMessageApiService.saveBatch(fillUserMessageInfo(userIdList,stationLetterInfo,pushPlan.getId()));
            }
            stationLetterInfo.setSendNumber(stationLetterInfo.getSendNumber() + userIdList.size());
            stationLetterApiService.update(stationLetterInfo);
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

    private Set<String> getVendor(List<UserInfoIndex> user,PushPlan pushPlan){
        Set<String> vendorClientId = new HashSet<>();
        if (user.size()>1024){
            List<List<UserInfoIndex>> partition = Lists.partition(Lists.newArrayList(user), 1024);
            for (List list : partition){
                Set vendor = this.getVendorClientId(list, pushPlan);
                if (CollectionUtils.isNotEmpty(vendor)){
                    vendorClientId.addAll(vendor);
                }
            }

        }else{
            Set vendor = this.getVendorClientId(user,pushPlan);
            if (CollectionUtils.isNotEmpty(vendor)){
                vendorClientId.addAll(vendor);
            }
        }
        return vendorClientId;
    }


    private List<UserInfoIndex> getOssUser(PushPlan pushPlan){
        Set<String> userPhoneSet = pushService.readExcelUserInfoMobiles(pushPlan.getOssUrl());
        List<UserInfoIndex> userInfoIndexList = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(userPhoneSet)){
            if (userPhoneSet.size() > ES_MAX_SIZE){
                List<List<String>> subUserInfos = Lists.partition(Lists.newArrayList(userPhoneSet),ES_MAX_SIZE);
                for (List<String> userPhones : subUserInfos){
                    List<UserInfoIndex> userInfoIndices = userInfoIndexApiService.findAll(ApiRequest.newInstance().filterIn(QUserInfoIndex.phone,userPhones));
                    userInfoIndexList.addAll(userInfoIndices);
                }
            }else {
                userInfoIndexList = userInfoIndexApiService.findAll(ApiRequest.newInstance().filterIn(QUserInfoIndex.phone,userPhoneSet));
            }
        }

        return userInfoIndexList;
    }

}
