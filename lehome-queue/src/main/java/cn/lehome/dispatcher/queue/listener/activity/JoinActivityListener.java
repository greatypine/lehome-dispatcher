package cn.lehome.dispatcher.queue.listener.activity;

import cn.lehome.base.api.business.activity.bean.activity.ApprenticeContributionStatistics;
import cn.lehome.base.api.business.activity.bean.activity.InviteApprenticeRecord;
import cn.lehome.base.api.business.activity.bean.activity.MasterApprenticeRelationship;
import cn.lehome.base.api.business.activity.bean.activity.QInviteApprenticeRecord;
import cn.lehome.base.api.business.activity.bean.advert.Advert;
import cn.lehome.base.api.business.activity.bean.advert.AdvertDeliverRange;
import cn.lehome.base.api.business.activity.bean.advert.AdvertPrizeResponse;
import cn.lehome.base.api.business.activity.bean.bonus.Bonus;
import cn.lehome.base.api.business.activity.bean.bonus.BonusItem;
import cn.lehome.base.api.business.activity.bean.card.*;
import cn.lehome.base.api.business.activity.bean.task.*;
import cn.lehome.base.api.business.activity.constant.JoinActivityTypeConstants;
import cn.lehome.base.api.business.activity.event.JoinActivityEventBean;
import cn.lehome.base.api.business.activity.service.activity.ApprenticeContributionStatisticsApiService;
import cn.lehome.base.api.business.activity.service.activity.InviteApprenticeRecordApiService;
import cn.lehome.base.api.business.activity.service.activity.MasterApprenticeRelationshipApiService;
import cn.lehome.base.api.business.activity.service.advert.ActivityAdvertRedisCache;
import cn.lehome.base.api.business.activity.service.advert.AdvertApiService;
import cn.lehome.base.api.business.activity.service.advert.AdvertDeliverRangeApiService;
import cn.lehome.base.api.business.activity.service.bonus.BonusApiService;
import cn.lehome.base.api.business.activity.service.card.*;
import cn.lehome.base.api.business.activity.service.task.UserSigninApiService;
import cn.lehome.base.api.business.activity.service.task.UserTaskApiService;
import cn.lehome.base.api.business.activity.service.task.UserTaskOperationRecordApiService;
import cn.lehome.base.api.business.activity.service.task.UserTaskRecordApiService;
import cn.lehome.base.api.common.bean.community.Community;
import cn.lehome.base.api.common.bean.device.ClientDevice;
import cn.lehome.base.api.common.bean.device.PushDeviceInfo;
import cn.lehome.base.api.common.component.message.push.PushComponent;
import cn.lehome.base.api.common.constant.MessageKeyConstants;
import cn.lehome.base.api.common.service.community.CommunityCacheApiService;
import cn.lehome.base.api.common.service.device.DeviceApiService;
import cn.lehome.base.api.user.bean.asset.UserBeanFlowInfo;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfo;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.asset.UserAssetApiService;
import cn.lehome.base.api.user.service.relationship.UserFriendApiService;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.business.activity.enums.advert.AdvertStatus;
import cn.lehome.bean.business.activity.enums.advert.AdvertType;
import cn.lehome.bean.business.activity.enums.advert.DeliverRangeType;
import cn.lehome.bean.business.activity.enums.bouns.BonusSourceType;
import cn.lehome.bean.business.activity.enums.bouns.BonusType;
import cn.lehome.bean.business.activity.enums.card.CardType;
import cn.lehome.bean.business.activity.enums.card.ChannelType;
import cn.lehome.bean.business.activity.enums.card.PrizeType;
import cn.lehome.bean.business.activity.enums.task.AssetType;
import cn.lehome.bean.business.activity.enums.task.InviteUserType;
import cn.lehome.bean.business.activity.enums.task.TaskType;
import cn.lehome.bean.business.activity.enums.task.UserTaskStatus;
import cn.lehome.bean.user.entity.enums.friend.FriendSourceType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.activity.ActivityService;
import cn.lehome.framework.base.api.core.compoment.redis.lock.RedisLock;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.exception.BaseApiException;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.bean.core.enums.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static cn.lehome.dispatcher.queue.service.activity.ActivityService.MASTER_APPRENTICE_CACHE_KEY;

/**
 * Created by wuzhao on 2018/3/15.
 */
public class JoinActivityListener extends AbstractJobListener {

    @Autowired
    private UserTaskApiService userTaskApiService;

    @Autowired
    private UserTaskRecordApiService userTaskRecordApiService;

    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService;

    @Autowired
    private BonusApiService bonusApiService;

    @Autowired
    private UserInfoApiService userInfoApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private DeviceApiService deviceApiService;

    @Autowired
    private PushComponent pushComponent;

    @Autowired
    private UserSigninApiService userSigninApiService;

    @Autowired
    private UserFriendApiService userFriendApiService;

    @Autowired
    private AdvertBegCardApiService advertBegCardApiServiceNew;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private AdvertApiService advertApiServiceNew;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ActivityAdvertRedisCache.ActivityCollectCardRedisCache activityCollectCardRedisCache;

    @Autowired
    private ActivityAdvertRedisCache activityAdvertRedisCache;

    @Autowired
    private AdvertCollectCardRecordApiService advertCollectCardRecordApiService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private AdvertCollectCardRecordCommunityRelationShipApiService advertCollectCardRecordCommunityRelationShipApiServiceNew;

    @Autowired
    private GainPrizeHistoryApiService gainPrizeHistoryApiServiceNew;

    @Autowired
    private ActivityCollectCardStatisticsCache activityCollectCardStatisticsCache;

    @Autowired
    private AdvertBegCardStatisticsApiService advertBegCardStatisticsApiServiceNew;

    @Autowired
    private UserAssetApiService userAssetApiService;

    @Autowired
    private UserTaskOperationRecordApiService userTaskOperationRecordApiService;

    @Autowired
    private MasterApprenticeRelationshipApiService masterApprenticeRelationshipApiService;

    @Autowired
    private InviteApprenticeRecordApiService inviteApprenticeRecordApiService;

    @Autowired
    private ApprenticeContributionStatisticsApiService apprenticeContributionStatisticsApiService;

    @Autowired
    private ActivityService activityService;

    @Autowired
    private AdvertDeliverRangeApiService advertDeliverRangeApiServiceNew;

    private static List<AdvertStatus> invalidAdvertStatus = Lists.newArrayList(AdvertStatus.OFFLINE, AdvertStatus.WAITING, AdvertStatus.OPENING,AdvertStatus.REFUNDING);


    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        SimpleEventMessage<JoinActivityEventBean> simpleEventMessage = (SimpleEventMessage<JoinActivityEventBean>) eventMessage;
        JoinActivityEventBean joinActivityEventBean = simpleEventMessage.getData();
        logger.error("joinActivityType : " + joinActivityEventBean.getJoinActivityType());
        switch (joinActivityEventBean.getJoinActivityType()) {
            case JoinActivityTypeConstants.DREW_FAMILY:
                this.drewFamily(joinActivityEventBean.getAttributes());
                break;
            case JoinActivityTypeConstants.USER_AUTH_HOUSE:
                this.userAuthHouse(joinActivityEventBean.getAttributes());
                break;
            case JoinActivityTypeConstants.DREW_BONUS:
                this.drewBonus(joinActivityEventBean.getAttributes());
                break;
            case JoinActivityTypeConstants.UPDATE_USER_DATA:
                this.updateUserData(joinActivityEventBean.getAttributes());
                break;
            case JoinActivityTypeConstants.FIRST_OPEN_DOOR :
                this.firstOpenDoor(joinActivityEventBean.getAttributes());
                break;
            default:
                break;
        }

    }

    private void firstOpenDoor(List<Object> attributes) {
        if (attributes.size() != 2) {
            logger.error("首次开门, 参数错误");
            return;
        }
        String phone = (String) attributes.get(0);
        Long communityExtId = (Long) attributes.get(1);
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByPhone(phone);

        CommonResult commonResult = userTaskApiService.finishCommonTask(phone, TaskType.FIRST_OPEN_DOOR, communityExtId);
        if (commonResult.getStatus().equals(EnableDisableStatus.ENABLE) && !commonResult.isDuplicateFinished()) {
            // 向用户发送消息
            UserTaskRecord userTask = userTaskApiService.findTaskByPhoneAndTaskType(phone, TaskType.FIRST_OPEN_DOOR);
            Map<String, String> params = Maps.newHashMap();
            params.put("amount", String.valueOf(divide(userTask.getAmount(), 100d, 2, BigDecimal.ROUND_HALF_UP)));
            params.put("taskType", TaskType.FIRST_OPEN_DOOR.getName());
            this.pushMessage(userInfoIndex.getClientId(), MessageKeyConstants.USER_TASK_COMPLETE, params, Maps.newHashMap());
        }
    }

    private void updateUserData(List<Object> attributes) {
        if (attributes.size() != 1) {
            logger.error("修改用户信息, 参数错误");
            return;
        }
        Long userId = (Long) attributes.get(0);
        UserInfo userInfo = userInfoApiService.findUserByUserId(userId);
        if (userInfo == null) {
            logger.error("用户信息未找到, userId = {}", userId);
            return;
        }

        String nickname = userInfo.getNickName();
        boolean updNickname = Pattern.matches("^半径[0-9]{6,8}", nickname) || Pattern.matches("^友邻[0-9]{6}", nickname);
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userId);

        if (StringUtils.isNotEmpty(userInfo.getIconUrl()) && !updNickname) {
            CommonResult commonResult = userTaskApiService.finishCommonTask(userInfo.getPhone(), TaskType.COMPLETE_USER_DATA, 0L);
            if (commonResult.getStatus().equals(EnableDisableStatus.ENABLE) && !commonResult.isDuplicateFinished()) {
                // 向用户发送消息
                UserTaskRecord userTask = userTaskApiService.findTaskByPhoneAndTaskType(userInfo.getPhone(), TaskType.COMPLETE_USER_DATA);
                Map<String, String> params = Maps.newHashMap();
                params.put("amount", String.valueOf(divide(userTask.getAmount(), 100d, 2, BigDecimal.ROUND_HALF_UP)));
                params.put("taskType", TaskType.COMPLETE_USER_DATA.getName());
                this.pushMessage(userInfoIndex.getClientId(), MessageKeyConstants.USER_TASK_COMPLETE, params, Maps.newHashMap());
            }
        }

    }

    @Override
    public String getConsumerId() {
        return "join_activity_message";
    }

    private void drewFamily(List<Object> attributes) {
        if (attributes.size() != 4) {
            logger.error("拉家人, 参数错误");
            return;
        }
        String phone = (String) attributes.get(0);
        String beInvitePhone = (String) attributes.get(1);
        InviteUserType inviteUserType = (InviteUserType) attributes.get(2);
        UserInfo userInfo = userInfoApiService.findByPhone(beInvitePhone);

        userTaskRecordApiService.changeInviteRecord(beInvitePhone, inviteUserType, phone);
        if (userInfo.getIsLogin().equals(YesNoStatus.YES)) {
            Long cid = (Long) attributes.get(3);
            UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.get(cid);
            LoginAuthResult loginAuthResult = userTaskApiService.auth(beInvitePhone, userHouseRelationship.getCommunityExtId());
            this.checkLoginResult(loginAuthResult, beInvitePhone);

        }
    }

    private void userAuthHouse(List<Object> attributes) {
        Long relationId = (Long) attributes.get(0);
        UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.get(relationId);
        if (userHouseRelationship == null) {
            logger.error("认证房产关系记录, 未找到, relationId = {}", relationId);
            return;
        }
        LoginAuthResult loginAuthResult = userTaskApiService.auth(userHouseRelationship.getRemark(), userHouseRelationship.getCommunityExtId());
        Long userId = userHouseRelationship.getUserId();
        if (userId != null && loginAuthResult.getStatus().equals(EnableDisableStatus.ENABLE) && !loginAuthResult.isDuplicateFinished()) {
            UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userId);
            UserTaskRecord userTaskRecord = userTaskApiService.findTaskByPhoneAndTaskType(userHouseRelationship.getRemark(), TaskType.LOGIN_AUTH);
            if (userTaskRecord != null && UserTaskStatus.FINISHED.equals(userTaskRecord.getStatus())) {
                Map<String, String> params = Maps.newHashMap();
                params.put("amount", String.valueOf(divide(userTaskRecord.getAmount(), 100d, 2, BigDecimal.ROUND_HALF_UP)));
                params.put("taskType", TaskType.LOGIN_AUTH.getName());
                // this.pushMessage(userInfoIndex.getClientId(), MessageKeyConstants.USER_TASK_COMPLETE, params, Maps.newHashMap());
            }
        }

        checkLoginResult(loginAuthResult, userHouseRelationship.getRemark());

    }

    /**
     * 领取奖励后的操作
     * 1 给被求卡人随机一张卡, 并且一次抽卡机会
     * 2 给求卡人一张卡
     * 3 建立两者的关系
     */
    private void drewBonus(List<Object> attributes) {
        if (attributes.size() != 1) {
            logger.error("获取登录礼包, 参数错误");
            return;
        }
        Long bonusId = (Long) attributes.get(0);
        logger.info("收到领取礼包消息：bonusId = {}", bonusId);
        Bonus bonus = bonusApiService.get(bonusId);
        LoginAuthResult loginAuthResult = userTaskApiService.firstLogin(bonus.getPhone());
        checkLoginResult(loginAuthResult, bonus.getPhone());

        UserInfoIndex beBegUser = userInfoIndexApiService.findByPhone(bonus.getPhone());
        List<BonusItem> bonusItems = bonusApiService.findBonusItem(bonusId);
        Collections.sort(bonusItems, (e1, e2)-> Long.compare( e2.getId(), e1.getId()));
        boolean ifDrawCardNumAdd = false;
        Long begCardId = 0L;
        for (BonusItem bonusItem : bonusItems) {
            if (!ifDrawCardNumAdd && BonusType.DRAW_CARD_NUM.equals(bonusItem.getBonusType()) && BonusSourceType.FIRST_LOGIN.equals(bonus.getType())) {
                userSigninApiService.addOrUpdateUserDrawCardRecord(beBegUser.getId(), bonusItem.getBonus().longValue());
                ifDrawCardNumAdd = true;
            }

            if (BonusType.BEAN_NUM.equals(bonusItem.getBonusType()) && BonusSourceType.FIRST_LOGIN.equals(bonus.getType())) {
                firstLoginGiveBean(beBegUser.getId(), bonusItem.getBonus());
            }

            if (bonusItem.getSourceId() != null && bonusItem.getSourceId() != 0L) {
                begCardId = bonusItem.getSourceId();
                break;
            }
        }

        // 创建师徒关系
        buildMasterRelation(beBegUser);

        if (begCardId == 0L) {
            logger.info("用户{}未参与求卡活动, 不继续后续操作!", beBegUser.getId());
            return;
        }

        AdvertBegCardInfo advertBegCardInfo = advertBegCardApiServiceNew.findOne(begCardId);

        // 求卡数据统计
        AdvertBegCardStatisticsInfo advertBegCardStatisticsInfo = advertBegCardStatisticsApiServiceNew.findOne(advertBegCardInfo.getUserId(),
                beBegUser.getId(), advertBegCardInfo.getAdvertId(), advertBegCardInfo.getCardType());
        if (advertBegCardStatisticsInfo == null) {
            logger.error("未找到求卡数据统计信息, userId={}, beInvitedUserId={}, advertId={}, cardType={}",
                    advertBegCardInfo.getUserId(), beBegUser.getId(), advertBegCardInfo.getAdvertId(), advertBegCardInfo.getCardType());
        } else {
            advertBegCardStatisticsApiServiceNew.updateStatus(advertBegCardStatisticsInfo.getId(), YesNoStatus.YES);
            activityCollectCardStatisticsCache.begCardUserLogin(advertBegCardInfo.getAdvertId(), beBegUser.getId());
        }

        UserInfoIndex begUser = userInfoIndexApiService.findByUserId(advertBegCardInfo.getUserId());
        // 被求卡人绑定和认证的小区
        List<Long> communityIds = getCommunityIds(beBegUser);
        Advert advert = checkCommunity(communityIds, advertBegCardInfo.getAdvertId());
        if (advert == null) {
            logger.info("用户%s绑定的小区不在集卡活动%s投放区域内, 不给当前用户卡!", beBegUser.getId(), advertBegCardInfo.getAdvertId());
        } else {
            // 给当前用户随机一张求卡人所在集卡活动的卡
            giveBeBegUserOneCard(communityIds, beBegUser, advert);
        }
        // 给求卡人所求的卡
        if (YesNoStatus.NO.equals(advertBegCardInfo.getDrawStatus())) {
            try {
                boolean flag = giveBegUserCard(advert, advertBegCardInfo.getUserId(), advertBegCardInfo.getCardType());
                if (flag) {
                    advertBegCardApiServiceNew.update(begCardId, beBegUser.getPhone(),YesNoStatus.YES);
                    Map<String, String> params = Maps.newHashMap();
                    Map<String, String> forwardMap = Maps.newHashMap();
                    String cardType = advertBegCardInfo.getCardType().name();
                    params.put("cardType", cardType);
                    forwardMap.put("advertId", advertBegCardInfo.getAdvertId().toString());
                    forwardMap.put("cardType", cardType);
                    this.pushMessage(begUser.getClientId(), MessageKeyConstants.GAIN_CARD_PRIZE, params, forwardMap);
                }
            } catch (IOException e) {
                logger.error(String.format("用户%s登录给求卡人%s一张卡操作失败!", beBegUser.getId(), begUser.getId()), e);
            }
        }
        // 创建好友关系
        userFriendApiService.save(begUser.getId(), beBegUser.getId(), FriendSourceType.BEGCARD);
        userFriendApiService.save(beBegUser.getId(), begUser.getId(), FriendSourceType.BEGCARD);


    }

    /**
     * 第一次登录奖励金豆
     */
    private void firstLoginGiveBean(Long userId, Integer bonus) {
        UserBeanFlowInfo userBeanFlowInfo = new UserBeanFlowInfo();
        userBeanFlowInfo.setUserId(userId);
        userBeanFlowInfo.setOperation(Operation.ADD);
        userBeanFlowInfo.setOperationNum(bonus.longValue());
        userBeanFlowInfo.setOperationType(OperationType.BEAN_GIFTS);
        userBeanFlowInfo.setOperationTime(new Date());
        userAssetApiService.operateBeanNum(userBeanFlowInfo);

        // 增加任务流水
        UserTaskOperationRecord userTaskOperationRecord = new UserTaskOperationRecord();
        userTaskOperationRecord.setOperationType(OperationType.BEAN_GIFTS);
        userTaskOperationRecord.setOperation(Operation.ADD);
        userTaskOperationRecord.setOperationNum(bonus.longValue());
        userTaskOperationRecord.setAssetType(AssetType.BEAN);
        userTaskOperationRecord.setObjectId(userId.toString());
        userTaskOperationRecord.setUserType(UserType.USER);
        userTaskOperationRecord.setOriginUserId(userId.toString());
        userTaskOperationRecordApiService.save(userTaskOperationRecord);
    }

    private void buildMasterRelation(UserInfoIndex beBegUser) {
        ApiRequest apiRequest = ApiRequest.newInstance().filterEqual(QInviteApprenticeRecord.beInvitedPhone, beBegUser.getPhone());
        List<InviteApprenticeRecord> list = inviteApprenticeRecordApiService.findAll(apiRequest);
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        if (list.size() > 1) {
            logger.error("师徒关系建立错误，beInvitedPhone={}", beBegUser.getPhone());
        }
        InviteApprenticeRecord record = list.get(0);

        MasterApprenticeRelationship relationship = new MasterApprenticeRelationship();
        relationship.setMasterId(record.getUserId());
        relationship.setApprenticeId(beBegUser.getId());
        relationship.setCreatedTime(new Date());

        String now = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        String key = String.format("%s%s_%s", MASTER_APPRENTICE_CACHE_KEY, record.getUserId(), now);
        RedisLock redisLock = new RedisLock(stringRedisTemplate, key, 5, TimeUnit.SECONDS);
        try {
            if (redisLock.tryLock()) {
                Integer apprenticeCount = activityService.getLastInviteInfoByMasterId(record.getUserId(), now);
                if (apprenticeCount >= 10) {
                    relationship.setIfGainMoney(YesNoStatus.NO);
                } else {
                    // 3.4.1 邀请徒弟不再奖励现金，只保留金豆进贡
                    relationship.setIfGainMoney(YesNoStatus.NO);
                }
                logger.info("创建师徒关系, masterId={}, apprenticeId={}", relationship.getMasterId(), relationship.getApprenticeId());
                masterApprenticeRelationshipApiService.save(relationship);
                userInfoIndexApiService.updateAsBeInvitedUser(beBegUser.getId(), true);
                userInfoIndexApiService.updateUserApprenticeNum(record.getUserId(), 1);
                activityService.addOneNewApprentice(record.getUserId(), now);
            } else {
                logger.error("增加徒弟缓存未获取到锁，apprenticeId="+ beBegUser.getId());
            }
        } catch (Exception e) {
            logger.error("增加徒弟缓存未获取到锁，apprenticeId="+ beBegUser.getId(), e);
        } finally {
            redisLock.unlock();
        }



        //插入徒弟贡献统计记录
        ApprenticeContributionStatistics contributionStatistics = apprenticeContributionStatisticsApiService.findOneByUserId(beBegUser.getId());
        if (contributionStatistics != null) {
            contributionStatistics.setInvitedTime(new Date());
            apprenticeContributionStatisticsApiService.update(contributionStatistics);
        } else {
            ApprenticeContributionStatistics statistics = new ApprenticeContributionStatistics();
            statistics.setUserId(beBegUser.getId());
            statistics.setTotalContributionBean(0L);
            statistics.setInvitedTime(new Date());
            apprenticeContributionStatisticsApiService.save(statistics);
        }
    }

    private void checkUserIfCollectCardBefore(Long advertId, Long userId, YesNoStatus isRobot) {
        Long count = advertCollectCardRecordApiService.countByUserIdAndIsRobot(userId, isRobot);
        if (count == null || count == 0L) {
            activityCollectCardStatisticsCache.newUserCollectCard(userId, advertId);
        }

    }

    private boolean giveBegUserCard(Advert advertInfo, Long begUserId, CardType cardType) throws IOException {
        if (invalidAdvertStatus.contains(advertInfo.getStatus())) {
            return false;
        }
        AdvertCollectCardRecord begUserCardInfo = advertCollectCardRecordApiService.findByAdvertIdAndUserIdAndIsRobot(advertInfo.getId(), begUserId, YesNoStatus.NO);
        if (begUserCardInfo == null) {
            // 检查用户是否之前集过卡并计入集卡统计
            checkUserIfCollectCardBefore(advertInfo.getId(), begUserId, YesNoStatus.NO);

            logger.warn("求卡人记录未找到，userId = {}, advertId = {}", begUserId, advertInfo.getId());
            begUserCardInfo = new AdvertCollectCardRecord();
            begUserCardInfo.setGainCardSort(activityAdvertRedisCache.getMaxGainCardSort(advertInfo.getId()));
            AdvertCollectCardCommonCacheBean commonCacheBean = activityCollectCardRedisCache.getCollectCardCommonData(advertInfo.getId());
            Map<CardType, Integer> cardNumber = Maps.newHashMap();
            for(CardType card : commonCacheBean.getCards().keySet()) {
                cardNumber.put(card, 0);
            }
            try {
                String cardNumberStr = objectMapper.writeValueAsString(cardNumber);
                begUserCardInfo.setCardsNumber(cardNumberStr);
            } catch (Exception e) {
                logger.error("json转换失败");
                begUserCardInfo.setCardsNumber("");
            }
            begUserCardInfo.setCards(0);
            begUserCardInfo.setCardCount(0);
            begUserCardInfo.setAdvertId(advertInfo.getId());
            begUserCardInfo.setIsRobot(YesNoStatus.NO);
            begUserCardInfo.setUserId(begUserId);
            begUserCardInfo = advertCollectCardRecordApiService.save(begUserCardInfo);
        }
        Map<CardType, Integer> cardNumberMap = objectMapper.readValue(begUserCardInfo.getCardsNumber(), new TypeReference<Map<CardType, Integer>>() {});
        for ( Map.Entry<CardType, Integer> entry : cardNumberMap.entrySet()){
            if (!entry.getKey().equals(cardType)) {
                continue;
            }
            if (entry.getValue() >= 1) {
                logger.info(String.format("用户%s已经获得集卡活动%s的%s, 不再给卡!", begUserId, advertInfo.getId(), cardType.getName()));
                return false;
            }
            cardNumberMap.put(entry.getKey(), 1);
        }
        begUserCardInfo.setCardsNumber(objectMapper.writeValueAsString(cardNumberMap));
        begUserCardInfo.setCardCount(begUserCardInfo.getCardCount() + 1);
        if ((begUserCardInfo.getCards() & cardType.getValue()) == 0) {
            begUserCardInfo.setCards(begUserCardInfo.getCards() ^ cardType.getValue());
        }
        advertCollectCardRecordApiService.update(begUserCardInfo);

        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(begUserId);
        List<Long> communityIds = getCommunityIds(userInfoIndex);
        final Long recordId = begUserCardInfo.getId();
        if (!CollectionUtils.isEmpty(communityIds)) {
            List<AdvertCollectCardRecordCommunityRelationShip> cardRecordCommunityRelationShipInfos =
                    advertCollectCardRecordCommunityRelationShipApiServiceNew.findByRecordId(begUserCardInfo.getId());
            if (cardRecordCommunityRelationShipInfos == null) {
                communityIds.stream().forEach(communityId -> {
                    AdvertCollectCardRecordCommunityRelationShip advertCollectCardRecordCommunityRelationshipInfo = new AdvertCollectCardRecordCommunityRelationShip();
                    advertCollectCardRecordCommunityRelationshipInfo.setRecordId(recordId);
                    advertCollectCardRecordCommunityRelationshipInfo.setCommunityId(communityId);
                    advertCollectCardRecordCommunityRelationShipApiServiceNew.save(advertCollectCardRecordCommunityRelationshipInfo);
                });
            } else {
                List<Long> oldCommunityIds = cardRecordCommunityRelationShipInfos.stream().map(AdvertCollectCardRecordCommunityRelationShip::getCommunityId).collect(Collectors.toList());
                communityIds.stream().forEach(communityId -> {
                    if (!oldCommunityIds.contains(communityId)) {
                        AdvertCollectCardRecordCommunityRelationShip advertCollectCardRecordCommunityRelationshipInfo = new AdvertCollectCardRecordCommunityRelationShip();
                        advertCollectCardRecordCommunityRelationshipInfo.setRecordId(recordId);
                        advertCollectCardRecordCommunityRelationshipInfo.setCommunityId(communityId);
                        advertCollectCardRecordCommunityRelationShipApiServiceNew.save(advertCollectCardRecordCommunityRelationshipInfo);
                    }
                });
            }
        }
        activityAdvertRedisCache.gainCard(begUserId, advertInfo.getId(), ChannelType.BEG_CARD, YesNoStatus.NO);

        logger.info("得卡内存统计, userId = {}, advertId = {}, cardType = {}", begUserCardInfo.getUserId(), begUserCardInfo.getAdvertId(), cardType.getName());
        activityCollectCardStatisticsCache.gainCard(begUserCardInfo.getUserId(), begUserCardInfo.getAdvertId(), cardType);

        return true;
    }

    /**
     * 被求卡人A一张卡, 如果求卡的活动投放范围不在A所在小区范围内,则不给A卡
     */
    private void giveBeBegUserOneCard(List<Long> communityIds, UserInfoIndex beBegUser, Advert advertInfo) {
        logger.info(String.format("求卡活动 advertId=%s, status= %s", advertInfo.getId(), advertInfo.getStatus()));
        if (invalidAdvertStatus.contains(advertInfo.getStatus())) {
            return;
        }

        String key = String.format("%s%s", ActivityAdvertRedisCache.USER_LOCK_PREFIX, beBegUser.getId());
        RedisLock redisLock = new RedisLock(stringRedisTemplate, key, 5, TimeUnit.SECONDS);
        Pair<Long, AdvertPrizeResponse> pair;
        try {
            if (redisLock.tryLock()) {
                pair = this.gainCard(communityIds, beBegUser.getId(), ChannelType.BE_BEG_CARD, advertInfo.getId(), YesNoStatus.NO);
                if (pair != null) {
                    activityAdvertRedisCache.gainCard(beBegUser.getId(), advertInfo.getId(), ChannelType.BE_BEG_CARD, YesNoStatus.NO);
                    logger.error(String.format("用户%s通过点击别人求卡链接获得活动%s的%s",
                            beBegUser.getId(), advertInfo.getId(), pair.getRight().getCardBean().getCardType().getName()));

                    logger.info("得卡内存统计, userId = {}, advertId = {}, cardType = {}", beBegUser.getId(), advertInfo.getId(), pair.getRight().getCardBean().getCardType());
                    activityCollectCardStatisticsCache.gainCard(beBegUser.getId(), advertInfo.getId(), pair.getRight().getCardBean().getCardType());
                }
            } else {
                logger.info("未获取到锁! redisLock.key={}", key);
            }
        } catch (Exception e) {
            logger.info("给被求卡人随机一张卡失败!", e);
        } finally {
            redisLock.unlock();
        }
    }

    public Pair<Long, AdvertPrizeResponse> gainCard(List<Long> communityIds, Long userId, ChannelType channelType, Long advertId, YesNoStatus isRobot) throws BaseApiException {

        AdvertCollectCardCommonCacheBean commonCacheBean = activityCollectCardRedisCache.getCollectCardCommonData(advertId);
        AdvertCollectCardExpireCacheBean expireCacheBean = activityCollectCardRedisCache.getCollectCardExpireData(advertId);
        //检查是否能够得卡
        long now = System.currentTimeMillis();
        if (now < commonCacheBean.getCollectStartTime() || now > commonCacheBean.getCollectEndTime()) {
            logger.warn("不在集卡时间范围内, 不能获得卡片");
            return null;
        }
        //根据概率获取卡片
        List<Pair<Integer, CardType>> probabilityList = Lists.newArrayList();
        for (CardType cardType : expireCacheBean.getCardProbability().keySet()) {
            Pair<Integer, CardType> pair = new ImmutablePair<>(expireCacheBean.getCardProbability().get(cardType), cardType);
            probabilityList.add(pair);
        }
        Collections.sort(probabilityList, (o1, o2) -> Integer.compare(o2.getKey(), o1.getKey()));
        Random random = new Random();
        Integer value = random.nextInt(100) + 1;
        Integer startProbability = 0;
        CardType card = null;
        for (Pair<Integer, CardType> pair : probabilityList) {
            Integer endProbability = startProbability + pair.getKey();
            if (value > startProbability && value <= endProbability) {
                card = pair.getValue();
                break;
            }
            startProbability = endProbability;
        }

        if (card == null) {
            logger.error("获取卡片失败");
            return null;
        }

        //判断获取的卡片是否是绑定卡
        logger.info("随机生成的卡为{}", card.getName());
        if (card.equals(commonCacheBean.getBindCard())) {
            logger.info("获取到是绑定卡，需要检查数量");
            Long bindCardCount = activityCollectCardRedisCache.increaseCount(advertId, 1L);
            if (bindCardCount == 0L || bindCardCount > commonCacheBean.getTotalGroupCount()) {
                //增加获取绑定卡失败或者已经超出总数，随机给予一张其他卡
                if (isRobot.equals(YesNoStatus.YES)) {
                    logger.error("机器人得到绑定卡, 重新得卡");
                }
            }

            //绑定卡已经发满，将内存中的获取卡概率修改，将绑定卡概率调整为0，并且将概率平均分配给其他卡
            if (bindCardCount >= commonCacheBean.getTotalGroupCount()) {
                logger.warn("已经发满绑定卡，修改获取卡概率");
                Map<CardType, Integer> cardProbability = expireCacheBean.getCardProbability();
                Integer avg = cardProbability.get(commonCacheBean.getBindCard()) / (cardProbability.size() - 1);
                boolean addOne = false;
                if (cardProbability.get(commonCacheBean.getBindCard()) % (cardProbability.size() - 1) != 0) {
                    addOne = true;
                }
                int addTimes = 0;
                for (CardType cardType : cardProbability.keySet()) {
                    if (cardType.equals(commonCacheBean.getBindCard())) {
                        cardProbability.put(cardType, 0);
                    } else {
                        addTimes++;
                        Integer newProbability = cardProbability.get(cardType) + avg;
                        if (addTimes == cardProbability.size() - 1 && addOne) {
                            newProbability = newProbability + 1;
                        }
                        cardProbability.put(cardType, newProbability);
                    }
                }
                try {
                    String cardProbabilityStr = objectMapper.writeValueAsString(cardProbability);
                    activityCollectCardRedisCache.modifyExpireCacheBean(advertId, ActivityAdvertRedisCache.ActivityCollectCardRedisCache.CARD_PROBABILITY_KEY, cardProbabilityStr);
                } catch (JsonProcessingException e) {
                    logger.error("json转换失败:", e);

                }
            }
        }
        //数据库操作
        try{
            //查找数据库中是否已有得卡记录
            AdvertCollectCardRecord recordInfo = advertCollectCardRecordApiService.findByCondition(advertId, userId, isRobot);
            boolean isNew = false;
            Map<CardType, Integer> cardNumberMap;
            if(recordInfo == null){
                // 检查用户是否之前集过卡并计入集卡统计
                checkUserIfCollectCardBefore(advertId, userId, YesNoStatus.NO);

                //没有得卡记录，新创建
                recordInfo = new AdvertCollectCardRecord();
                recordInfo.setAdvertId(advertId);
                recordInfo.setUserId(userId);
                recordInfo.setIsRobot(isRobot);
                recordInfo.setCards(card.getValue());
                cardNumberMap = Maps.newHashMap();
                int cardCount = 0;
                for (CardType cardType : commonCacheBean.getCards().keySet()) {
                    if (cardType.equals(card)) {
                        if (cardType == commonCacheBean.getBindCard() && isRobot.equals(YesNoStatus.YES)) {
                            logger.error("机器人首次得绑定卡, 卡数量虚加1");
                            cardNumberMap.put(cardType, 2);
                            cardCount += 2;
                        } else {
                            cardNumberMap.put(cardType, 1);
                            cardCount += 1;
                        }
                    } else {
                        cardNumberMap.put(cardType, 0);
                    }
                }
                recordInfo.setCardCount(cardCount);
                recordInfo.setCardsNumber(objectMapper.writeValueAsString(cardNumberMap));
                recordInfo.setOpened(YesNoStatus.NO);
                recordInfo.setAmount(0L);
                recordInfo.setGainCardSort(activityAdvertRedisCache.getMaxGainCardSort(advertId));
                recordInfo = advertCollectCardRecordApiService.save(recordInfo);
                logger.info("为用户创建得卡记录, recordId={}", recordInfo.getId());
                isNew = true;
            }else{
                //有得卡记录，修改得卡值和得卡数量
                logger.info("更新用户得卡记录, recordId={}", recordInfo.getId());
                int cardCount = recordInfo.getCardCount();
                if ((recordInfo.getCards() & card.getValue()) == 0) {
                    recordInfo.setCards(recordInfo.getCards() ^ card.getValue());
                }
                cardNumberMap = objectMapper.readValue(recordInfo.getCardsNumber(), new TypeReference<Map<CardType, Integer>>() {});
                if (card == commonCacheBean.getBindCard() && cardNumberMap.get(card) == 0 && isRobot.equals(YesNoStatus.YES)) {
                    cardNumberMap.put(card, cardNumberMap.get(card) + 2);
                    cardCount += 2;
                } else {
                    cardNumberMap.put(card, cardNumberMap.get(card) + 1);
                    cardCount += 1;
                }
                recordInfo.setCardsNumber(objectMapper.writeValueAsString(cardNumberMap));
                recordInfo.setCardCount(cardCount);
                recordInfo = advertCollectCardRecordApiService.update(recordInfo);
            }
            if(isNew){
                List<AdvertCollectCardRecordCommunityRelationShip> toBeSaveList = Lists.newArrayList();
                for (Long commId : communityIds) {
                    AdvertCollectCardRecordCommunityRelationShip advertCollectCardRecordCommunityRelationshipInfo = new AdvertCollectCardRecordCommunityRelationShip();
                    advertCollectCardRecordCommunityRelationshipInfo.setRecordId(recordInfo.getId());
                    advertCollectCardRecordCommunityRelationshipInfo.setCommunityId(commId);
                    toBeSaveList.add(advertCollectCardRecordCommunityRelationshipInfo);
                }
                advertCollectCardRecordCommunityRelationShipApiServiceNew.save(toBeSaveList);
            }else{
                List<AdvertCollectCardRecordCommunityRelationShip>  relationshipInfoList = advertCollectCardRecordCommunityRelationShipApiServiceNew.findByRecordId(recordInfo.getId());
                List<Long> oldCommunityList = relationshipInfoList.stream().map(AdvertCollectCardRecordCommunityRelationShip::getCommunityId).collect(Collectors.toList());
                for (Long commId : communityIds) {
                    if (!oldCommunityList.contains(commId)) {
                        AdvertCollectCardRecordCommunityRelationShip advertCollectCardRecordCommunityRelationshipInfo = new AdvertCollectCardRecordCommunityRelationShip();
                        advertCollectCardRecordCommunityRelationshipInfo.setRecordId(recordInfo.getId());
                        advertCollectCardRecordCommunityRelationshipInfo.setCommunityId(commId);
                        advertCollectCardRecordCommunityRelationShipApiServiceNew.save(advertCollectCardRecordCommunityRelationshipInfo);
                    }
                }
            }

            //添加得卡记录
            GainPrizeHistory historyEntity = this.addGainHistory(userId, advertId, channelType,
                    PrizeType.CARD, card, isRobot, null, communityIds.get(0));
            logger.info("添加得卡记录, historyId={}", historyEntity.getId() );
            //设置返回结果
            AdvertPrizeResponse response = new AdvertPrizeResponse();
            List<CardImage> cardImageList = Lists.newArrayList(commonCacheBean.getCards().values());
            Collections.sort(cardImageList, (o1, o2) -> Integer.compare(o1.getCardType().getValue(), o2.getCardType().getValue()));
            List<CardBean> cardBeanList = Lists.newArrayList();
            for (CardImage cardImage : cardImageList) {
                Integer number = cardNumberMap.get(cardImage.getCardType());
                if (number == null) {
                    number = 0;
                }
                CardBean cardBean = new CardBean();
                cardBean.setBigCardUrl(cardImage.getBigImageUrl());
                cardBean.setGrayCardUrl(cardImage.getGrayCardUrl());
                cardBean.setSmallCardUrl(cardImage.getSmallImageUrl());
                cardBean.setHttpUrl(cardImage.getHttpUrl());
                cardBean.setCardType(cardImage.getCardType());
                cardBean.setCardName(cardImage.getName());
                cardBean.setCardNumber(number);
                cardBeanList.add(cardBean);
            }
            response.setId(recordInfo.getId());
            response.setAdvertId(commonCacheBean.getAdvertId());
            response.setType(AdvertType.CARD_COLLECTING);
            response.setCardBeanList(cardBeanList);
            CardBean cardBean = new CardBean();
            cardBean.setCardType(card);
            cardBean.setBigCardUrl(commonCacheBean.getCards().get(card).getBigImageUrl());
            cardBean.setSmallCardUrl(commonCacheBean.getCards().get(card).getSmallImageUrl());
            cardBean.setGrayCardUrl(commonCacheBean.getCards().get(card).getGrayCardUrl());
            cardBean.setCardName(commonCacheBean.getCards().get(card).getName());
            cardBean.setHttpUrl(commonCacheBean.getCards().get(card).getHttpUrl());
            int gainCount = 0;
            for (CardType cardType : commonCacheBean.getCards().keySet()) {
                if ((recordInfo.getCards() & cardType.getValue()) != 0) {
                    gainCount += 1;
                }
            }
            cardBean.setCardNumber(commonCacheBean.getCards().size() - gainCount);
            response.setCardBean(cardBean);
            return new ImmutablePair<>(historyEntity.getId(), response);
        }catch (Exception e){
            logger.info("获取卡数据库操作失败:", e);
            if (card.equals(commonCacheBean.getBindCard())) {
                logger.warn("退还一张绑定卡");
                activityCollectCardRedisCache.increaseCount(advertId, -1L);
            }
        }
        return null;
    }

    private GainPrizeHistory addGainHistory(Long userId, Long advertId, ChannelType type, PrizeType prizeType, CardType cardType, YesNoStatus isRobot, Long redPacketId, Long communityId) {
        GainPrizeHistory gainPrizeHistoryInfo =  new GainPrizeHistory();
        gainPrizeHistoryInfo.setIsRobot(isRobot);
        gainPrizeHistoryInfo.setUserId(userId);
        gainPrizeHistoryInfo.setAdvertId(advertId);
        gainPrizeHistoryInfo.setPrizeType(prizeType);
        gainPrizeHistoryInfo.setChannelType(type);
        gainPrizeHistoryInfo.setCommunityId(communityId);
        if (prizeType.equals(PrizeType.RED_PACKET)) {
            gainPrizeHistoryInfo.setObject(redPacketId);
        } else {
            gainPrizeHistoryInfo.setObject((long) cardType.getValue());
        }
        gainPrizeHistoryInfo = gainPrizeHistoryApiServiceNew.save(gainPrizeHistoryInfo);
        return gainPrizeHistoryInfo;
    }

    /**
     * 检查集卡活动是否在
     */
    private Advert checkCommunity(List<Long> communityIds, Long advertId) {
        Set<Long> regionIds = Sets.newHashSet();
        Map<Long, Community> communities = communityCacheApiService.findAllCommunity(communityIds);
        if (communities != null) {
            for (Community community : communities.values()) {
                regionIds.add(community.getPcode());
                regionIds.add(Long.valueOf(community.getCitycode()));
                regionIds.add(community.getAdcode());
            }
        }

        Advert advertInfo = advertApiServiceNew.findOne(advertId);
        List<AdvertDeliverRange> deliveryRanges = advertDeliverRangeApiServiceNew.findByAdvertId(advertId);
        for (AdvertDeliverRange advertDeliveryRange : deliveryRanges) {
            if (advertDeliveryRange.getType().getValue() == DeliverRangeType.REGION.getValue()) {
                if (advertDeliveryRange.getTargetId().equals(100000L)) {
                    return advertInfo;
                }
                if (regionIds.contains(advertDeliveryRange.getTargetId())) {
                    return advertInfo;
                }
            } else {
                if (communityIds.contains(advertDeliveryRange.getTargetId())) {
                    return advertInfo;
                }
            }
        }
        return null;
    }

    private List<Long> getCommunityIds(UserInfoIndex userInfoIndex) {
        List<Long> bindCommunityIds = userInfoIndex.getBindCommunityIds();
        List<Long> communityExtIds = userInfoIndex.getAuthCommunityIds();
        List<Long> communityIds = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(bindCommunityIds)) {
            communityIds.addAll(bindCommunityIds);
        }
        if (!CollectionUtils.isEmpty(communityExtIds)) {
            communityExtIds.forEach(communityExtId -> {
                List<Long> extBindCommunityIds = communityCacheApiService.getCommunityExtBind(communityExtId);
                if (!CollectionUtils.isEmpty(extBindCommunityIds)) {
                    extBindCommunityIds.stream().forEach(communityId -> {
                        if (!communityIds.contains(communityId)) {
                            communityIds.add(communityId);
                        }
                    });
                }
            });
        }
        return communityIds;
    }

    private void checkLoginResult(LoginAuthResult loginAuthResult, String phone) {
        if (loginAuthResult.getStatus().equals(EnableDisableStatus.ENABLE) && !loginAuthResult.isDuplicateFinished()) {
            logger.error("认证任务正常完成");
            // 发送消息
            if (loginAuthResult.isCheckFriends()) {
                InviteResult inviteResult = userTaskApiService.checkInviteFriends(phone);
                if (!inviteResult.isDuplicateFinished() && inviteResult.isInviteRecord() && inviteResult.getStatus().equals(EnableDisableStatus.ENABLE) && inviteResult.isComplete()) {
                    String invitePhone = inviteResult.getPhone();
                    // 向邀请人发送消息
                    UserInfoIndex userInfoIndex = userInfoIndexApiService.findByPhone(invitePhone);
                    InviteUserRecord inviteUserRecord = userTaskApiService.findOneInviteUserRecord(invitePhone, phone, InviteUserType.FRIEND);
                    Map<String, String> params = Maps.newHashMap();
                    params.put("amount", String.valueOf(divide(inviteUserRecord.getAmount(), 100d, 2, BigDecimal.ROUND_HALF_UP)));
                    params.put("taskType", TaskType.INVITE_FRIEND.getName());
                    this.pushMessage(userInfoIndex.getClientId(), MessageKeyConstants.USER_TASK_COMPLETE, params, Maps.newHashMap());

                    // 建立好友关系
                    UserInfoIndex beInvitedUser = userInfoIndexApiService.findByPhone(phone);
                    userFriendApiService.save(beInvitedUser.getId(), userInfoIndex.getId(), FriendSourceType.INVITE_FRIEND);
                    userFriendApiService.save(userInfoIndex.getId(), beInvitedUser.getId(), FriendSourceType.INVITE_FRIEND);
                }
            }
            if (loginAuthResult.isCheckFamilyAndNeighbor()) {
                InviteResult inviteResult = userTaskApiService.checkInviteFamilyOrNeighbor(phone);
                if (!inviteResult.isDuplicateFinished() && inviteResult.isInviteRecord() && inviteResult.getStatus().equals(EnableDisableStatus.ENABLE) && inviteResult.isComplete()) {
                    String invitePhone = inviteResult.getPhone();
                    // 向邀请人发送消息
                    UserInfoIndex userInfoIndex = userInfoIndexApiService.findByPhone(invitePhone);
                    InviteUserRecord inviteUserRecord = userTaskApiService.findOneInviteUserRecord(phone);
                    Map<String, String> params = Maps.newHashMap();
                    params.put("amount", String.valueOf(divide(inviteUserRecord.getAmount(), 100d, 2, BigDecimal.ROUND_HALF_UP)));
                    params.put("taskType", TaskType.INVITE_NEIGHBOR.getName());
                    this.pushMessage(userInfoIndex.getClientId(), MessageKeyConstants.USER_TASK_COMPLETE, params, Maps.newHashMap());
                }
            }
        }
    }

    public BigDecimal divide(double a1, double a2, int scale, int roundingMode) {
        BigDecimal num1 = new BigDecimal(a1);
        BigDecimal num2 = new BigDecimal(a2);
        return num1.divide(num2, scale, roundingMode);

    }

    /**
     *  推送消息
     * @param clientId clientID
     * @param messageKey message类型
     * @param params 参数
     * @param forwardParams APP传递参数
     */
    private void pushMessage(String clientId, String messageKey, Map<String, String> params, Map<String, String> forwardParams){
        try {
            ClientDevice clientDevice = deviceApiService.getClientDevice(ClientType.SQBJ, clientId);
            if (clientDevice == null) {
                logger.error("设备信息未找到, clientId = {}", clientId);
                return;
            }
            PushDeviceInfo pushDeviceInfo = deviceApiService.getPushDeviceInfo(clientDevice.getId(), PushVendorType.JIGUANG);
            if (pushDeviceInfo == null) {
                logger.error("推送信息未找到, clientId = {}", clientDevice.getId());
                return;
            }

            pushComponent.pushSingle(pushDeviceInfo.getVendorClientId(), messageKey, params, forwardParams, clientDevice.getClientOSType().equals(ClientOSType.ANDROID) ? PushOsType.ANDROID : PushOsType.IOS);
        } catch (Exception e) {
            logger.error(String.format("推送消息失败! clientId = %s", clientId), e);
        }
    }

}
