package cn.lehome.dispatcher.queue.listener.activity;

import cn.lehome.base.api.business.activity.bean.advert.AdvertInfoResponse;
import cn.lehome.base.api.business.activity.service.advert.AdvertApiService;
import cn.lehome.base.api.business.activity.service.advert.AdvertRedPacketAllocateApiService;
import cn.lehome.base.api.business.activity.service.card.AdvertCollectCardRecordApiService;
import cn.lehome.bean.business.activity.enums.advert.AdvertType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author yanwenkai
 * @date 2018/11/7
 */
public class AdvertStatisticsOfflineListener extends AbstractJobListener {

    @Autowired
    private AdvertApiService advertApiServiceNew;

    @Autowired
    private AdvertCollectCardRecordApiService advertCollectCardRecordApiServiceNew;

    @Autowired
    private AdvertRedPacketAllocateApiService advertRedPacketAllocateApiServiceNew;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        try {
            if (eventMessage == null) {
                logger.error("消息中的对象数据为空");
                return;
            }
            LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
            Long advertId = longEventMessage.getData();
            logger.error("广告下线检查合并统计数据advertId:{}",advertId);
            AdvertInfoResponse oneSimple = advertApiServiceNew.findOneSimple(advertId);
            if (!oneSimple.getType().equals(AdvertType.CARD_COLLECTING)) {
                // 红包合并统计数据
                advertRedPacketAllocateApiServiceNew.mergeStatisticsCacheToDB(advertId);
                advertRedPacketAllocateApiServiceNew.checkStatistics(advertId);
            } else {
                // 集卡合并统计数据
                this.lastCacheMerge(advertId);
                this.checkStatistics(advertId);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void lastCacheMerge(Long advertId) {
        logger.error("广告下线, 检查是否还有未合并的临时统计数据, advertId = {}", advertId);
        advertCollectCardRecordApiServiceNew.mergeStatisticsCacheToDB(advertId);
    }

    private void checkStatistics(Long advertId) {
        logger.error("检查统计结果信息, advertId = {}", advertId);
        advertCollectCardRecordApiServiceNew.checkStatistics(advertId);
    }

    @Override
    public String getConsumerId() {
        return "collect_card_statistics_offline";
    }
}
