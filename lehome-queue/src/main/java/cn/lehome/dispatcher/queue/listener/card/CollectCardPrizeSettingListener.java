package cn.lehome.dispatcher.queue.listener.card;

import cn.lehome.base.api.business.activity.service.advert.ActivityAdvertRedisCache;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author yanwenkai
 * @date 2018/10/18
 */
public class CollectCardPrizeSettingListener extends AbstractJobListener {

    @Autowired
    private ActivityAdvertRedisCache activityAdvertRedisCache;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (eventMessage == null) {
            logger.error("消息中的对象数据为空");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        logger.error("初始化集卡奖金");
        Long advertId = longEventMessage.getData();
        activityAdvertRedisCache.initPrize(advertId);
    }

    @Override
    public String getConsumerId() {
        return "collect_card_prize_setting_message";
    }
}
