package cn.lehome.dispatcher.quartz.service.invoke.card;

import cn.lehome.base.api.activity.bean.advert.Advert;
import cn.lehome.base.api.activity.bean.advert.QAdvert;
import cn.lehome.base.api.activity.bean.card.AdvertCollectCardCommonCacheBean;
import cn.lehome.base.api.activity.bean.card.AdvertCollectCardStatistics;
import cn.lehome.base.api.activity.bean.card.AdvertStatisticsCacheBean;
import cn.lehome.base.api.activity.bean.card.CardImage;
import cn.lehome.base.api.activity.service.advert.ActivityAdvertRedisCache;
import cn.lehome.base.api.activity.service.advert.AdvertApiService;
import cn.lehome.base.api.activity.service.advert.AdvertRedPacketAllocateApiService;
import cn.lehome.base.api.activity.service.card.ActivityCollectCardStatisticsCache;
import cn.lehome.base.api.activity.service.card.AdvertCollectCardStatisticsApiService;
import cn.lehome.bean.activity.enums.advert.AdvertStatus;
import cn.lehome.bean.activity.enums.advert.AdvertType;
import cn.lehome.bean.activity.enums.card.CardType;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yanwenkai
 * @date 2018/10/22
 */
@Service("collectCardStatisticsSchedulerService")
public class CollectCardStatisticsSchedulerServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private AdvertApiService advertApiServiceNew;

    @Autowired
    private ActivityCollectCardStatisticsCache activityCollectCardStatisticsCache;

    @Autowired
    private AdvertCollectCardStatisticsApiService advertCollectCardStatisticsApiServiceNew;

    @Autowired
    private ActivityAdvertRedisCache.ActivityCollectCardRedisCache activityCollectCardRedisCache;

    @Autowired
    private AdvertRedPacketAllocateApiService advertRedPacketAllocateApiService;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${advertising.collect.card.statistics.enable}")
    private Boolean enable;

    private static final List<AdvertType> RED_PACKET_LIST = Lists.newArrayList(AdvertType.RED_PACKET, AdvertType.THEME_RED_PACKET,
            AdvertType.ADVERT_PIC, AdvertType.BEAN_PACKET, AdvertType.SPECIAL_PACKET);

    @Override
    public void doInvoke(Map<String, String> params) {
        if (enable) {
            //统计集卡中统计入库
            ApiRequest apiRequest = ApiRequest.newInstance();
            apiRequest.filterEqual(QAdvert.status, AdvertStatus.PUBLISHED);
            apiRequest.filterEqual(QAdvert.type, AdvertType.CARD_COLLECTING);
            List<Advert> advertList = advertApiServiceNew.findAll(apiRequest);
            advertList.stream().forEach(this::cardToDB);

            //统计开奖中统计入库
            apiRequest = ApiRequest.newInstance();
            apiRequest.filterEqual(QAdvert.status, AdvertStatus.OPENING);
            apiRequest.filterEqual(QAdvert.type, AdvertType.CARD_COLLECTING);
            advertList = advertApiServiceNew.findAll(apiRequest);
            advertList.stream().forEach(this::cardToDB);

            apiRequest = ApiRequest.newInstance();
            apiRequest.filterEqual(QAdvert.status, AdvertStatus.PUBLISHED);
            apiRequest.filterIn(QAdvert.type, RED_PACKET_LIST);
            advertList = advertApiServiceNew.findAll(apiRequest);
            advertList.stream().forEach(this::redPacketToDB);

        }

    }

    private void redPacketToDB(Advert advert) {
        logger.info("红包临时缓存数据合并并入库, advertId = {}", advert.getId());
        advertRedPacketAllocateApiService.mergeStatisticsCacheToDB(advert.getId());
    }

    private void cardToDB(Advert advert) {
        logger.error("临时统计数据合并到数据库中, advertId = {}, status = {}", advert.getId(), advert.getStatus());
        Long advertId = advert.getId();
        AdvertStatisticsCacheBean tempBean = activityCollectCardStatisticsCache.getTemp(advertId);
        if (tempBean == null) {
            logger.warn("临时统计缓存无数据，不需要入库, advertId = {}", advertId);
            return ;
        }
        activityCollectCardStatisticsCache.mergeTempCache(tempBean);
        AdvertStatisticsCacheBean bean = activityCollectCardStatisticsCache.getStay(advertId);
        AdvertCollectCardStatistics statistics = advertCollectCardStatisticsApiServiceNew.findOne(advertId);
        AdvertCollectCardCommonCacheBean commonCacheBean = activityCollectCardRedisCache.getCollectCardCommonData(advertId);
        List<CardImage> cardImageList = Lists.newArrayList(commonCacheBean.getCards().values());
        Collections.sort(cardImageList, (o1, o2) -> Integer.compare(o1.getCardType().getValue(), o2.getCardType().getValue()));
        if (statistics == null) {
            statistics = new AdvertCollectCardStatistics();
        }
        statistics.setAdvertId(advertId);
        statistics.setDrewAmount(bean.getDrewAmount());
        statistics.setPullNewUserNumber(Long.valueOf(bean.getPullNewUserNumber()));
        statistics.setCardNumber(bean.getCardNumber());
        statistics.setStealCount(bean.getStealCount());
        statistics.setStealPeopleNumber(bean.getStealCardUserIds() == null ? 0 : bean.getStealCardUserIds().size());

        statistics.setOpenDoorNum(bean.getOpenDoorCount());
        statistics.setOpenDoorUserNum(bean.getOpenDoorUserIds() == null ? 0L : Long.valueOf(bean.getOpenDoorUserIds().size()));

        statistics.setBegCardNumber(bean.getBegCardCount());
        statistics.setBegCardUserNum(bean.getBegCardUserIds() == null ? 0L : Long.valueOf(bean.getBegCardUserIds().size()));

        statistics.setDrawCardNum(bean.getDrawCardCount());
        statistics.setDrawCardUserNum(bean.getDrawCardUserIds() == null ? 0L : Long.valueOf(bean.getDrawCardUserIds().size()));

        statistics.setPullNewUserNumber(bean.getPullNewUserNumber());
        statistics.setPullUserSuccessNum(bean.getPullUserSuccessNum());

        statistics.setGainPrizeNum(bean.getGainPrizeNum());

        Integer peopleNumber = 0;
        Integer collectPeopleNumber = 0;
        String peopleDetailNumberStr = "";
        if (bean.getPeopleCardsDetail() != null) {
            peopleNumber = bean.getPeopleCardsDetail().size();
            Map<Integer, Integer> cardNumberPeopleMap = Maps.newHashMap();
            for (Integer cards : bean.getPeopleCardsDetail().values()) {
                if (cards.intValue() == commonCacheBean.getIsAllStatus().intValue()) {
                    collectPeopleNumber++;
                }
                int number = 0;
                for (CardType cardType : commonCacheBean.getCards().keySet()) {
                    if ((cards & cardType.getValue()) != 0) {
                        number++;
                    }
                }
                if (cardNumberPeopleMap.containsKey(number)) {
                    cardNumberPeopleMap.put(number, cardNumberPeopleMap.get(number) + 1);
                } else {
                    cardNumberPeopleMap.put(number, 1);
                }
            }
            try {
                LinkedHashMap<Integer, Integer> linkedHashMap = Maps.newLinkedHashMap();
                for (int i = 1; i <= cardImageList.size(); i++) {
                    int number = 0;
                    if (cardNumberPeopleMap.get(i) != null) {
                        number = cardNumberPeopleMap.get(i);
                    }
                    linkedHashMap.put(i, number);
                }
                peopleDetailNumberStr = objectMapper.writeValueAsString(linkedHashMap);
            } catch (JsonProcessingException e) {
                logger.error("json转换失败");
            }
        }
        statistics.setCollectPeopleNumber(collectPeopleNumber);
        statistics.setCardOfPeopleDetail(peopleDetailNumberStr);
        statistics.setPeopleNumber(peopleNumber);
        statistics.setExposureNumber(0);
        String cardDetailStr = "";
        if (bean.getCardCountDetail() != null) {
            try {
                Map<CardType, Integer> cardNumberMap = Maps.newHashMap();
                for (CardType cardType : commonCacheBean.getCards().keySet()) {
                    Integer number = bean.getCardCountDetail().get(cardType) == null? 0 : bean.getCardCountDetail().get(cardType);
                    cardNumberMap.put(cardType, number);
                }
                LinkedHashMap<CardType, Integer> linkedHashMap = Maps.newLinkedHashMap();
                for (CardImage cardImage : cardImageList) {
                    linkedHashMap.put(cardImage.getCardType(), cardNumberMap.get(cardImage.getCardType()));
                }
                cardDetailStr = objectMapper.writeValueAsString(linkedHashMap);
            } catch (JsonProcessingException e) {
                logger.error("json转换失败");
            }
        }
        statistics.setCardOfCountDetail(cardDetailStr);
        try {
            advertCollectCardStatisticsApiServiceNew.saveOrUpdate(statistics);
        } catch (Exception e) {
            logger.error("统计结果入库失败: ", e);
        } finally {
            activityCollectCardStatisticsCache.putLastUpdateTime(advertId);
        }
    }
}
