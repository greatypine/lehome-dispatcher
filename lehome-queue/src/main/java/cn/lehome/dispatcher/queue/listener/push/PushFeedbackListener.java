package cn.lehome.dispatcher.queue.listener.push;

import cn.lehome.base.api.common.bean.push.PushSendRecord;
import cn.lehome.base.api.common.operation.bean.push.PushPlan;
import cn.lehome.base.api.common.operation.bean.push.PushSendInformation;
import cn.lehome.base.api.common.operation.bean.push.QPushPlan;
import cn.lehome.base.api.common.operation.service.push.PushPlanApiService;
import cn.lehome.base.api.common.operation.service.push.PushSendInformationApiService;
import cn.lehome.base.api.common.service.job.ScheduleJobApiService;
import cn.lehome.base.api.common.service.push.PushSendRecordApiService;
import cn.lehome.dispatcher.queue.listener.AbstractSessionJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.bean.core.enums.PushType;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class PushFeedbackListener extends AbstractSessionJobListener {

    @Autowired
    private PushSendRecordApiService pushSendRecordApiService;

    @Autowired
    private PushSendInformationApiService pushSendInformationApiService;

    @Autowired
    private PushPlanApiService pushPlanApiService;

    @Autowired
    private ScheduleJobApiService scheduleJobApiService;

    private static final int maxNumber = 20;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)){
            logger.error("消息类型不对");
            return;
        }

        try{

            List<PushPlan> pushPlanList = pushPlanApiService.findAll(ApiRequest.newInstance().filterEqual(QPushPlan.pushType, PushType.PUSH).filterLessThan(QPushPlan.scheduleJobTimes, maxNumber).filterLessThan(QPushPlan.pushTime,new Date()));
            logger.error("集合大小" + pushPlanList.size());

            if (CollectionUtils.isNotEmpty(pushPlanList)) {
                pushPlanList.stream().forEach(pushPlan -> {
                    logger.error("pushPlan的id" + pushPlan.getId());

                    List<PushSendRecord> pushSendRecordList = pushSendRecordApiService.findAllByPlanId(pushPlan.getId());
                    for (PushSendRecord pushSendRecord : pushSendRecordList) {
                        pushSendRecordApiService.update(pushSendRecord);
                    }
                    List<PushSendRecord> allByPlanId = pushSendRecordApiService.findAllByPlanId(pushPlan.getId());
                    int preSendNum = allByPlanId.stream().collect(Collectors.summingInt(PushSendRecord::getPreSendNum));
                    int realSendNum = allByPlanId.stream().collect(Collectors.summingInt(PushSendRecord::getRealSendNum));
                    int arriveNum = allByPlanId.stream().collect(Collectors.summingInt(PushSendRecord::getArriveNum));
                    int openNum = allByPlanId.stream().collect(Collectors.summingInt(PushSendRecord::getOpenNum));

                    PushSendInformation pushSendInformation = pushSendInformationApiService.findByPlanId(pushPlan.getId());

                    if (pushSendInformation == null) {
                        pushSendInformation = new PushSendInformation();
                        pushSendInformation.setPlanId(pushPlan.getId());
                        pushSendInformation.setPreSendNum(preSendNum);
                        pushSendInformation.setArriveNum(arriveNum);
                        pushSendInformation.setRealSendNum(realSendNum);
                        pushSendInformation.setOpenNum(openNum);
                        pushSendInformationApiService.save(pushSendInformation);
                    } else {
                        pushSendInformation.setPreSendNum(preSendNum);
                        pushSendInformation.setArriveNum(arriveNum);
                        pushSendInformation.setRealSendNum(realSendNum);
                        pushSendInformation.setOpenNum(openNum);
                        pushSendInformationApiService.save(pushSendInformation);
                    }
                    logger.error("更新前次数" + pushPlan.getScheduleJobTimes());
                    pushPlan.setScheduleJobTimes(pushPlan.getScheduleJobTimes() + 1);
                    pushPlanApiService.update(pushPlan);
                    logger.error("更新后次数" + pushPlan.getScheduleJobTimes());


                });
            }



        }catch (Exception e){
            logger.error("更新推送数据失败",e.getMessage());
        }
    }

    @Override
    public String getConsumerId() {
        return "push_feedback";
    }
}
