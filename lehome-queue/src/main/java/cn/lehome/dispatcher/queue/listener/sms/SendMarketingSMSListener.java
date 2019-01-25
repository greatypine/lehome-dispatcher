package cn.lehome.dispatcher.queue.listener.sms;

import cn.lehome.base.api.tool.bean.push.PushPlan;
import cn.lehome.base.api.tool.service.message.MessageSendApiService;
import cn.lehome.base.api.tool.service.push.PushPlanApiService;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.push.PushService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.bean.core.enums.SendStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Created by zhuhai on 2018/7/13.
 */
public class SendMarketingSMSListener extends AbstractJobListener {

    @Autowired
    private PushPlanApiService pushPlanApiService;

    @Autowired
    private PushService pushService;

    @Autowired
    private MessageSendApiService messageSendApiService;

    @Value("${marketing.sms.templateId}")
    private Long templateId;

    private static final Integer SEND_MAX_SIZE = 1000;

    private static final Integer ES_MAX_COUNT = 1024;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        //等待2秒确保数据库里事务处理完成
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            logger.error(e.toString(), e);
        }
        PushPlan pushPlan = pushPlanApiService.findOne(longEventMessage.getData());
        if (pushPlan == null) {
            logger.error("发送营销短信任务未找到, id = {}", longEventMessage.getData());
            return;
        }
        pushPlan.setSendStatus(SendStatus.DEALING);
        pushPlan = pushPlanApiService.update(pushPlan);
        boolean isDeal = true;

        try {
            Set<String> mobileSet = Sets.newHashSet();
            if (StringUtils.isNotEmpty(pushPlan.getOssUrl())) {
                //读取待发送用户
                Set<String> mobiles = pushService.readExcelUserInfoMobiles(pushPlan.getOssUrl());
                mobileSet.addAll(mobiles);
            }
            if (StringUtils.isNotEmpty(pushPlan.getRegionIds()) || StringUtils.isNotEmpty(pushPlan.getCommunityIds())) {
                //查询待发送用户
                Map<String,List<UserInfoIndex>> userInfoMaps = pushService.findAllUserInfo(pushPlan);
                List<UserInfoIndex> userInfoIndexList =userInfoMaps.get("commonUserInfos");
                if(!userInfoIndexList.isEmpty()) {
                    Set<String> mobiles = userInfoIndexList.stream().map(UserInfoIndex::getPhone).collect(Collectors.toSet());
                    mobileSet.addAll(mobiles);
                }
            }
            if (!mobileSet.isEmpty()) {
                //发送营销短信
                this.sendBatchSms(mobileSet, pushPlan);
            }
        } catch (Exception e) {
            logger.error("发送营销短信失败", e);
            isDeal = false;
        } finally {
            if (isDeal) {
                pushPlan.setSendStatus(SendStatus.DEAL);
                pushPlanApiService.update(pushPlan);
            } else {
                pushPlan.setSendStatus(SendStatus.DEAL_FAILED);
                pushPlanApiService.update(pushPlan);
            }
        }
    }

    /**
     * 批量发送营销短信
     *
     * @param mobileSet
     * @param pushPlan
     */
    private void sendBatchSms(Set<String> mobileSet, PushPlan pushPlan) {

        //组织参数content用来替换模板中的content变量
        Map<String, String> params = Maps.newHashMap();
        params.put("content", pushPlan.getContent());

        //去重后的手机号集合
        List<String> mobileList = Lists.newArrayList();
        mobileList.addAll(mobileSet);

        if (!mobileList.isEmpty()) {
            //短信厂商一次最多发1000个手机号
            if (mobileList.size() > SEND_MAX_SIZE) {
                List<List<String>> partition = Lists.partition(mobileList, SEND_MAX_SIZE);
                for (List<String> mobileSubList : partition) {
                    // 发送营销短信
                    messageSendApiService.sendBatchSms(mobileSubList, templateId, pushPlan.getId(), params);
                    // 商厂要求每次发包间隔1-2秒
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException e) {
                        logger.error(e.toString(), e);
                    }
                }
            } else {
                // 发送营销短信
                messageSendApiService.sendBatchSms(mobileList, templateId, pushPlan.getId(), params);
            }
        }

    }

    @Override
    public String getConsumerId() {
        return "MARKETING_SMS";
    }
}
