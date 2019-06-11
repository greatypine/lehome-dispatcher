package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.business.activity.bean.advert.*;
import cn.lehome.base.api.business.activity.service.advert.AdvertApiService;
import cn.lehome.base.api.business.activity.service.advert.AdvertDeliverTimelineApiService;
import cn.lehome.base.api.business.activity.service.advert.AdvertRedPacketAllocateApiService;
import cn.lehome.base.api.business.activity.service.advert.AdvertRedPacketPolicyApiService;
import cn.lehome.base.api.common.bean.job.ScheduleJob;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.base.api.common.service.job.ScheduleJobApiService;
import cn.lehome.base.api.common.util.DateUtil;
import cn.lehome.bean.business.activity.enums.advert.AdvertStatus;
import cn.lehome.bean.business.activity.enums.advert.AdvertTimeLineType;
import cn.lehome.bean.business.activity.enums.advert.AdvertType;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import net.sf.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 更新活动状态定时任务
 * Created by zuoguodong on 2018/4/20
 */
@Service("advertStatusAutoUpdateService")
public class AdvertStatusAutoUpdateServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private AdvertApiService advertApiService;

    @Autowired
    private AdvertDeliverTimelineApiService advertDeliverTimelineApiService;

    @Autowired
    private AdvertRedPacketAllocateApiService advertRedPacketAllocateApiService;

    @Autowired
    private AdvertRedPacketPolicyApiService advertRedPacketPolicyApiService;

    @Autowired
    private ScheduleJobApiService scheduleJobApiService;

    @Autowired
    private EventBusComponent eventBusComponent;

    private static final List<AdvertType> RED_PACKET_LIST = Lists.newArrayList(AdvertType.RED_PACKET, AdvertType.THEME_RED_PACKET,
            AdvertType.ADVERT_PIC, AdvertType.BEAN_PACKET, AdvertType.SPECIAL_PACKET);

    @Override
    public void doInvoke(Map<String, String> params) {
        logger.info("定时更新活动状态任务start");

        ApiRequest apiRequest = ApiRequest.newInstance();
        apiRequest.filterEqual(QAdvert.status, AdvertStatus.AUDITED);
        List<Advert> advertList = advertApiService.findAll(apiRequest);
        List<Advert> needChangeAdverts = filterToBePublishedByTimeLine(advertList);
        logger.info("查询需要更新为已发布的活动有{}个", needChangeAdverts.size());
        if (!CollectionUtils.isEmpty(needChangeAdverts)) {
            needChangeAdverts.stream().forEach(e -> {
                if (RED_PACKET_LIST.contains(e.getType())){
                    AdvertStatusChangeBean changeBean = new AdvertStatusChangeBean();
                    changeBean.setReason("自动发布");
                    changeBean.setStatus(AdvertStatus.PUBLISHED);
                    advertApiService.updateStatus(e.getId(), changeBean);
                    createSendRedPacketTimePush(e.getId(),e.getType());
                } else if (AdvertType.CARD_COLLECTING.equals(e.getType())) {
                    AdvertStatusChangeBean changeBean = new AdvertStatusChangeBean();
                    changeBean.setReason("集卡自动发布");
                    changeBean.setStatus(AdvertStatus.PUBLISHED);
                    advertApiService.updateStatus(e.getId(), changeBean);
                }
            });
        }

        apiRequest = ApiRequest.newInstance();
        apiRequest.filterEqual(QAdvert.status, AdvertStatus.PUBLISHED);
        advertList = advertApiService.findAll(apiRequest);
        needChangeAdverts = filterToBeOfflineByTimeLine(advertList);
        logger.info("查询需要下线的红包活动有{}个", needChangeAdverts.size());
        if (!CollectionUtils.isEmpty(needChangeAdverts)) {
            needChangeAdverts.stream().forEach(e -> {
                AdvertStatusChangeBean changeBean = new AdvertStatusChangeBean();
                changeBean.setReason("自动下线");
                changeBean.setStatus(AdvertStatus.OFFLINE);
                advertApiService.updateStatus(e.getId(), changeBean);
                sendOfflineMessage(e.getId());
            });
        }

        needChangeAdverts = filterToBeWaitingByTimeLine(advertList);
        logger.info("查询集卡完成需要等待开奖的集卡活动有{}个",needChangeAdverts.size());
        if (!CollectionUtils.isEmpty(needChangeAdverts)) {
            needChangeAdverts.stream().forEach(e ->{
                AdvertStatusChangeBean changeBean = new AdvertStatusChangeBean();
                changeBean.setReason("集卡时间到期, 等待开奖");
                changeBean.setStatus(AdvertStatus.WAITING);
                advertApiService.updateStatus(e.getId(), changeBean);
                sendMessage(e.getId());
            });
        }

        apiRequest = ApiRequest.newInstance();
        apiRequest.filterEqual(QAdvert.type, AdvertType.CARD_COLLECTING);
        apiRequest.filterEqual(QAdvert.status, AdvertStatus.WAITING);
        advertList = advertApiService.findAll(apiRequest);
        needChangeAdverts = filterToBeOpeningByTimeLine(advertList);
        logger.info("查询等待开奖需要开奖的集卡活动有{}个",needChangeAdverts.size());
        if (!CollectionUtils.isEmpty(needChangeAdverts)) {
            needChangeAdverts.stream().forEach(e ->{
                AdvertStatusChangeBean changeBean = new AdvertStatusChangeBean();
                changeBean.setReason("开始开奖集卡活动");
                changeBean.setStatus(AdvertStatus.OPENING);
                advertApiService.updateStatus(e.getId(), changeBean);
            });
        }

        apiRequest = ApiRequest.newInstance();
        apiRequest.filterEqual(QAdvert.type, AdvertType.CARD_COLLECTING);
        apiRequest.filterEqual(QAdvert.status, AdvertStatus.OPENING);
        advertList = advertApiService.findAll(apiRequest);
        needChangeAdverts = filterToBeOfflineByTimeLine(advertList);
        logger.info("查询开奖中需要下线的集卡活动有{}个",needChangeAdverts.size());
        if (!CollectionUtils.isEmpty(needChangeAdverts)) {
            needChangeAdverts.stream().forEach(e ->{
                AdvertStatusChangeBean changeBean = new AdvertStatusChangeBean();
                changeBean.setReason("集卡开奖时间到期, 自动下线");
                changeBean.setStatus(AdvertStatus.OFFLINE);
                advertApiService.updateStatus(e.getId(), changeBean);
                sendOfflineMessage(e.getId());
            });
        }
        logger.info("定时更新活动状态任务end");
    }

    private List<Advert> filterToBeOpeningByTimeLine(List<Advert> advertList) {
        List<Advert> adverts = Lists.newArrayList();
        advertList.stream().forEach(e -> {
            List<AdvertDeliverTimeline> timelineList = advertDeliverTimelineApiService.findAllByAdvertId(e.getId());
            for (AdvertDeliverTimeline advertDeliverTimeline : timelineList) {
                boolean check = false;
                if (AdvertType.CARD_COLLECTING.equals(e.getType())) {
                    if (AdvertTimeLineType.RED_PACKET.equals(advertDeliverTimeline.getType())) {
                        check = true;
                    }
                }
                if (check && DateUtil.isBeforeNow(advertDeliverTimeline.getStartDate())) {
                    adverts.add(e);
                }
            }
        });
        return adverts;
    }

    private List<Advert> filterToBeWaitingByTimeLine(List<Advert> advertList) {
        List<Advert> adverts = Lists.newArrayList();
        advertList.stream().forEach(e -> {
            List<AdvertDeliverTimeline> timelineList = advertDeliverTimelineApiService.findAllByAdvertId(e.getId());
            for (AdvertDeliverTimeline advertDeliverTimeline : timelineList) {
                boolean check = false;
                if (AdvertType.CARD_COLLECTING.equals(e.getType())) {
                    if (AdvertTimeLineType.CARD.equals(advertDeliverTimeline.getType())) {
                        check = true;
                    }
                }
                if (check && DateUtil.isBeforeNow(advertDeliverTimeline.getEndDate())) {
                    adverts.add(e);
                }
            }
        });
        return adverts;
    }

    private List<Advert> filterToBePublishedByTimeLine(List<Advert> advertList) {
        List<Advert> adverts = Lists.newArrayList();
        advertList.stream().forEach(e -> {
            List<AdvertDeliverTimeline> timelineList = advertDeliverTimelineApiService.findAllByAdvertId(e.getId());
            for (AdvertDeliverTimeline advertDeliverTimeline : timelineList) {
                boolean check = false;
                if (RED_PACKET_LIST.contains(e.getType())) {
                    check = true;
                } else if (AdvertType.CARD_COLLECTING.equals(e.getType())) {
                    if (AdvertTimeLineType.CARD.equals(advertDeliverTimeline.getType())) {
                        check = true;
                    }
                }
                if (check && DateUtil.isBeforeNow(advertDeliverTimeline.getStartDate())) {
                        adverts.add(e);
                }
            }
        });
        return adverts;
    }

    private List<Advert> filterToBeOfflineByTimeLine(List<Advert> advertList) {
        List<Advert> adverts = Lists.newArrayList();
        advertList.stream().forEach(e -> {
            List<AdvertDeliverTimeline> timelineList = advertDeliverTimelineApiService.findAllByAdvertId(e.getId());
            for (AdvertDeliverTimeline advertDeliverTimeline : timelineList) {
                boolean check = false;
                if (RED_PACKET_LIST.contains(e.getType())) {
                    check = true;
                } else if (AdvertType.CARD_COLLECTING.equals(e.getType())) {
                    if (AdvertTimeLineType.RED_PACKET.equals(advertDeliverTimeline.getType())) {
                        check = true;
                    }
                }
                if (check) {
                    if (DateUtil.isBeforeNow(advertDeliverTimeline.getEndDate())) {
                        //1.1 时间到了自动下线
                        adverts.add(e);
                    } else {
                        if (AdvertType.ADVERT_PIC.equals(e.getType()) || AdvertType.CARD_COLLECTING.equals(e.getType())) {
                            // 广告图没有投放个数，会被1.2直接下线  集卡也直接跳过 否则查出来的openedNum为null 抛异常
                            continue;
                        }
                        //1.2 时间没到但是已经领取完并且全部打开了自动下线
                        Long openedNum = advertRedPacketAllocateApiService.countByAdvertIdAndDrewAndOpened(e.getId(), YesNoStatus.YES, YesNoStatus.YES);
                        AdvertRedPacketPolicy redPacketPolicy = advertRedPacketPolicyApiService.findOne(e.getId());
                        if (redPacketPolicy.getDeliverCount().compareTo(openedNum) <= 0) {
                            adverts.add(e);
                        }
                    }
                }
            }
        });
        return adverts;
    }

    private void createSendRedPacketTimePush(Long advertId,AdvertType type) {
        if (AdvertType.SPECIAL_PACKET.equals(type) || AdvertType.THEME_RED_PACKET.equals(type)||AdvertType.RED_PACKET.equals(type) || AdvertType.BEAN_PACKET.equals(type)){
            List<AdvertDeliverTimeline> allByAdvertId = advertDeliverTimelineApiService.findAllByAdvertId(advertId);
            if (!CollectionUtils.isEmpty(allByAdvertId)) {
                AdvertDeliverTimeline timeline = allByAdvertId.get(0);
                Date endDate = timeline.getEndDate();
                logger.info("插入红包将要过期定时任务。。advertId:{}",advertId);
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(endDate);
                calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) - 6);
                String cronExpression = String.format("%s %s %s %s %s %s %s", calendar.get(Calendar.SECOND), calendar.get(Calendar.MINUTE), calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.DATE), calendar.get(Calendar.MONTH) + 1, "?", calendar.get(Calendar.YEAR)+"-"+calendar.get(Calendar.YEAR));
                ScheduleJob scheduleJob = new ScheduleJob();
                scheduleJob.setIsSync(YesNoStatus.YES);
                scheduleJob.setIsOnce(YesNoStatus.YES);
                scheduleJob.setJobGroup("redPacketWillExpire-sendPush-group");
                scheduleJob.setJobName("redPacketWillExpire-sendPush-job"+advertId);
                scheduleJob.setAliasName("红包将要过期推送定时");
                scheduleJob.setCronExpression(cronExpression);
                scheduleJob.setDescription("红包将要过期推送定时");
                JSONObject object = new JSONObject();
                object.put("advertId",advertId.toString());
                scheduleJob.setExeParams(JSON.toJSONString(object));
                scheduleJob.setExeServiceName("redPacketWillExpireSendPushService");
                scheduleJobApiService.create(scheduleJob);
            }
        }
    }

    private void sendMessage(Long advertId) {
        logger.info("发送消息生成集卡缓存奖金，advertId:{}",advertId);
        eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.COLLECT_CARD_PRIZE_SETTING_MESSAGE_EVENT,advertId));
    }

    private void sendOfflineMessage(Long advertId) {
        logger.info("营销活动下线发送消息统计数据，advertId:{}",advertId);
        eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.ADVERT_STATISTICS_OFFLINE_MESSAGE_EVENT,advertId));
    }
}
