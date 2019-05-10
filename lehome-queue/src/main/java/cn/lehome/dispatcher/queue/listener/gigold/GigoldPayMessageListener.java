package cn.lehome.dispatcher.queue.listener.gigold;

import cn.lehome.base.api.advertising.bean.advert.AdvertInfo;
import cn.lehome.base.api.advertising.bean.advert.AdvertResponse;
import cn.lehome.base.api.advertising.bean.card.AdvertCollectCardRecordInfo;
import cn.lehome.base.api.advertising.bean.income.IncomeFailed;
import cn.lehome.base.api.advertising.bean.redPacket.AdvertRedPacketAllocateResponse;
import cn.lehome.base.api.advertising.bean.task.InviteUserRecord;
import cn.lehome.base.api.advertising.bean.task.UserTaskRecord;
import cn.lehome.base.api.advertising.constant.PubConstant;
import cn.lehome.base.api.advertising.service.advert.AdvertApiService;
import cn.lehome.base.api.advertising.service.card.AdvertCollectCardRecordApiService;
import cn.lehome.base.api.advertising.service.income.IncomeFailedApiService;
import cn.lehome.base.api.advertising.service.redPacket.AdvertRedPacketApiService;
import cn.lehome.base.api.advertising.service.task.UserTaskRecordApiService;
import cn.lehome.base.api.advertising.utils.RandomUtil;
import cn.lehome.base.api.business.ec.bean.special.SpecialActivityCapitalInvestment;
import cn.lehome.base.api.business.ec.bean.special.SpecialActivityCapitalInvestmentBillingHistory;
import cn.lehome.base.api.business.ec.bean.special.SpecialUserInfo;
import cn.lehome.base.api.business.ec.service.special.SpecialActivityCapitalInvestmentApiService;
import cn.lehome.base.api.business.ec.service.special.SpecialActivityCapitalInvestmentBillingHistoryApiService;
import cn.lehome.base.api.thirdparty.bean.gigold.RedPacketGetIncomeHttpResponse;
import cn.lehome.base.api.thirdparty.compoment.gigold.GigoldPaymentComponent;
import cn.lehome.base.api.tool.event.GiGoldPayBean;
import cn.lehome.base.api.user.bean.user.UserInfo;
import cn.lehome.base.api.user.service.user.UserInfoApiService;
import cn.lehome.bean.advertising.enums.advert.IncomeType;
import cn.lehome.bean.business.enums.admaster.DealingsType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.compoment.redis.lock.RedisLock;
import cn.lehome.framework.base.api.core.enums.Constants;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class GigoldPayMessageListener extends AbstractJobListener {


    @Autowired
    private GigoldPaymentComponent gigoldPaymentComponent;

    @Autowired
    private IncomeFailedApiService incomeFailedApiService;

    @Autowired
    private UserInfoApiService userInfoApiService;

    @Autowired
    private UserTaskRecordApiService userTaskRecordApiService;

    @Autowired
    private SpecialActivityCapitalInvestmentApiService specialActivityCapitalInvestmentApiService;

    @Autowired
    private AdvertRedPacketApiService advertRedPacketApiService;

    private Long NEW_HAND_SPECIAL_ACTIVITY_ID = 1L;

    @Value("${advertising.notify.amount}")
    private Long notifyAmount;

    private static long lastNotifyTime = 0l;

    @Value("${advertising.gigold.sqbj.merchantNo}")
    private String sqbjPlatformMerchantNo;

    private static final String GIGOLD_AMOUNT_PREFIX = "gigold.amount.prefix:";

    @Autowired
    private SpecialActivityCapitalInvestmentBillingHistoryApiService specialActivityCapitalInvestmentBillingHistoryApiService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private AdvertApiService advertApiService;

    @Autowired
    private AdvertCollectCardRecordApiService advertCollectCardRecordApiService;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof SimpleEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<GiGoldPayBean> simpleEventMessage = (SimpleEventMessage) eventMessage;
        GiGoldPayBean giGoldPayBean = simpleEventMessage.getData();

        switch (giGoldPayBean.getType()) {
            case INVITE_USER_TASK:
                this.inviteTaskDrewAmount(giGoldPayBean.getId());
                break;
            case NEW_HAND_TASK:
                this.userTaskDrewAmount(giGoldPayBean.getId());
                break;
            case RED_PACKET:
                this.redPacketIncome(giGoldPayBean.getId());
                break;
            case COLLECT_CARD:
                this.collectCardIncome(giGoldPayBean.getId());
                break;
            default:
                break;
        }


    }

    public void redPacketIncome(Long redPacketId) {
        AdvertRedPacketAllocateResponse advertRedPacketAllocateResponse = advertRedPacketApiService.findOne(redPacketId);
        if (advertRedPacketAllocateResponse == null) {
            logger.error("红包信息未找到，redPacketId = {}", redPacketId);
            return;
        }
        if (StringUtils.isNotEmpty(advertRedPacketAllocateResponse.getTxNumber())) {
            logger.error("红包已经入账，redPacketId = {}", redPacketId);
            this.txnumberExists(redPacketId, IncomeType.RED_PACKET, advertRedPacketAllocateResponse.getAdvertId());
            return;
        }
        AdvertInfo advertInfo = advertApiService.findOne(advertRedPacketAllocateResponse.getAdvertId());
        if (advertInfo == null) {
            logger.error("广告信息未找到，advertId = {}", advertRedPacketAllocateResponse.getAdvertId());
            saveFailedRecord(redPacketId, IncomeType.RED_PACKET, "99999", "广告信息未找到，advertId = " + advertRedPacketAllocateResponse.getAdvertId(), advertRedPacketAllocateResponse.getAdvertId());
            return;
        }
        String txNumber = this.findFailedTxNumber(advertInfo.getId(), redPacketId, IncomeType.RED_PACKET, Long.parseLong(advertRedPacketAllocateResponse.getDrewUser()));
        if (txNumber == null) {
            return;
        }

        String thirdTxNumber = gigoldIncome(advertRedPacketAllocateResponse.getDrewUser(),  advertInfo.getOrderNo(), advertRedPacketAllocateResponse.getAmount(), txNumber, redPacketId, IncomeType.RED_PACKET, advertRedPacketAllocateResponse.getId());
        if (thirdTxNumber == null) {
            return;
        }

        advertRedPacketApiService.fillTxNumber(redPacketId, txNumber, thirdTxNumber);
    }

    public void collectCardIncome(Long recordId) {
        AdvertCollectCardRecordInfo advertCollectCardRecordInfo = advertCollectCardRecordApiService.findOne(recordId);
        if (advertCollectCardRecordInfo == null) {
            logger.error("集卡记录未找到, recordId = {}", recordId);
            return;
        }
        if (StringUtils.isNotEmpty(advertCollectCardRecordInfo.getTxNumber())) {
            logger.error("奖金已经入账，recordId = {}", recordId);
            this.txnumberExists(recordId, IncomeType.COLLECT_CARD, advertCollectCardRecordInfo.getAdvertId());
            return;
        }
        AdvertInfo advertInfo = advertApiService.findOne(advertCollectCardRecordInfo.getAdvertId());
        if (advertInfo == null) {
            logger.error("广告信息未找到，advertId = {}", advertCollectCardRecordInfo.getAdvertId());
            saveFailedRecord(recordId, IncomeType.COLLECT_CARD, "99999", "广告信息未找到，advertId = " + advertCollectCardRecordInfo.getAdvertId(), advertCollectCardRecordInfo.getAdvertId());
            return;
        }

        String txNumber = this.findFailedTxNumber(advertInfo.getId(), recordId, IncomeType.RED_PACKET, advertCollectCardRecordInfo.getUserId());
        if (txNumber == null) {
            return;
        }

        String thirdTxNumber = gigoldIncome(advertCollectCardRecordInfo.getUserId().toString(),  advertInfo.getOrderNo(), advertCollectCardRecordInfo.getAmount(), txNumber, recordId, IncomeType.COLLECT_CARD, advertCollectCardRecordInfo.getAdvertId());
        if (thirdTxNumber == null) {
            return;
        }
        advertCollectCardRecordApiService.fillTxNumber(recordId, txNumber, thirdTxNumber);
    }

    public void userTaskDrewAmount(Long userTaskId) {
        UserTaskRecord userTaskRecord = userTaskRecordApiService.getRecord(userTaskId);
        if (userTaskRecord == null) {
            logger.error("用户任务记录未找到，userTaskId = {}", userTaskId);
            return;
        }
        if (StringUtils.isNotEmpty(userTaskRecord.getTxNumber())) {
            logger.error("红包已经入账，userTaskId = {}", userTaskId);
            this.txnumberExists(userTaskId, IncomeType.NEW_HAND_TASK, 0L);
            return;
        }
        List<SpecialActivityCapitalInvestment> capitalInvestmentEntityList = specialActivityCapitalInvestmentApiService.findBySpecialActivityIdAndStatusIn(NEW_HAND_SPECIAL_ACTIVITY_ID);
        long totalAmount = 0L;
        boolean isAmountEnough = false;
        Long capitalInvestmentId = 0L;
        if (capitalInvestmentEntityList != null && capitalInvestmentEntityList.size() != 0) {
            Collections.sort(capitalInvestmentEntityList, ((o1, o2) -> Long.compare(o1.getAmount(), o2.getAmount())));
            for (SpecialActivityCapitalInvestment capitalInvestment : capitalInvestmentEntityList) {
                totalAmount += capitalInvestment.getAmount();
                if (capitalInvestment.getAmount() >= userTaskRecord.getAmount()) {
                    isAmountEnough = true;
                    if (capitalInvestmentId == 0L) {
                        capitalInvestmentId = capitalInvestment.getId();
                    }
                }
            }
        }
        RedisLock redisLock = new RedisLock(stringRedisTemplate, GIGOLD_AMOUNT_PREFIX + capitalInvestmentId, 5000l, TimeUnit.MILLISECONDS);
        try {
            if (!redisLock.tryLock()) {
                saveFailedRecord(userTaskId, IncomeType.NEW_HAND_TASK, "88888", "redis所错误，taskId = " + userTaskId, 0L);
            } else {
                if (totalAmount < notifyAmount) {
                    if (lastNotifyTime == 0l || !DateUtils.isSameDay(new Date(lastNotifyTime), new Date(System.currentTimeMillis()))) {
                        logger.error("发送资金余额不足通知");
                        //TODO this.sendMail(totalAmount);
                        lastNotifyTime = System.currentTimeMillis();
                    }
                }
                if (!isAmountEnough) {
                    logger.error("没有足够资金来给用户, userTaskId = {}", userTaskId);
                    saveFailedRecord(userTaskId, IncomeType.NEW_HAND_TASK, "66666", "没有足够资金来给用户，userTaskId = " + userTaskId, 0L);
                    return;
                }

                String phone = userTaskRecord.getPhone();

                String txNumber = this.findFailedTxNumber(0L, userTaskId, IncomeType.NEW_HAND_TASK, Long.parseLong(phone));
                SpecialActivityCapitalInvestment specialActivityCapitalInvestment = specialActivityCapitalInvestmentApiService.get(capitalInvestmentId);


                String thirdTxNumber = gigoldIncomeByPhone(phone,  specialActivityCapitalInvestment.getOrderNumber(), userTaskRecord.getAmount(), txNumber, userTaskId, IncomeType.NEW_HAND_TASK, 0L);
                if (thirdTxNumber == null) {
                    return;
                }
                specialActivityCapitalInvestmentApiService.plusAmount(capitalInvestmentId, userTaskRecord.getAmount());
                userTaskRecordApiService.fillUserTaskDrewTx(userTaskId, txNumber);
                this.saveSpecialActivityBillingHistory(txNumber, thirdTxNumber, userTaskRecord.getAmount(), specialActivityCapitalInvestment);
            }
        } catch (Exception e) {
            logger.error("走账失败", e);
            saveFailedRecord(userTaskId, IncomeType.NEW_HAND_TASK, "88888", "新手任务获取失败，taskId = " + userTaskId, 0L);
        } finally {
            redisLock.unlock();
        }

    }



    @Override
    public String getConsumerId() {
        return "gigold_pay";
    }

    public void inviteTaskDrewAmount(Long inviteRecordId) {
        InviteUserRecord inviteUserRecord = userTaskRecordApiService.getInvite(inviteRecordId);
        if (inviteUserRecord == null) {
            logger.error("邀请记录未找到，inviteRecordId = {}", inviteRecordId);
            return;
        }
        if (StringUtils.isNotEmpty(inviteUserRecord.getTxNumber())) {
            logger.error("红包已经入账，inviteRecordId = {}", inviteRecordId);
            this.txnumberExists(inviteRecordId, IncomeType.INVITE_USER_TASK, 0L);
            return;
        }
        List<SpecialActivityCapitalInvestment> capitalInvestmentEntityList = specialActivityCapitalInvestmentApiService.findBySpecialActivityIdAndStatusIn(NEW_HAND_SPECIAL_ACTIVITY_ID);
        long totalAmount = 0L;
        boolean isAmountEnough = false;
        Long capitalInvestmentId = 0L;
        if (capitalInvestmentEntityList != null && capitalInvestmentEntityList.size() != 0) {
            Collections.sort(capitalInvestmentEntityList, ((o1, o2) -> Long.compare(o1.getAmount(), o2.getAmount())));
            for (SpecialActivityCapitalInvestment capitalInvestment : capitalInvestmentEntityList) {
                totalAmount += capitalInvestment.getAmount();
                if (capitalInvestment.getAmount() >= inviteUserRecord.getAmount()) {
                    isAmountEnough = true;
                    if (capitalInvestmentId == 0L) {
                        capitalInvestmentId = capitalInvestment.getId();
                    }
                }
            }
        }
        RedisLock redisLock = new RedisLock(stringRedisTemplate, GIGOLD_AMOUNT_PREFIX + capitalInvestmentId, 5000l, TimeUnit.MILLISECONDS);
        try {
            if (!redisLock.tryLock()) {
                saveFailedRecord(inviteRecordId, IncomeType.INVITE_USER_TASK, "88888", "redis所错误，inviteRecordId = " + inviteRecordId, 0L);
            } else {
                if (totalAmount < notifyAmount) {
                    if (lastNotifyTime == 0l || !DateUtils.isSameDay(new Date(lastNotifyTime), new Date(System.currentTimeMillis()))) {
                        logger.error("发送资金余额不足通知");
                        //TODO SEND MAIL this.sendMail(totalAmount);
                        lastNotifyTime = System.currentTimeMillis();
                    }
                }
                if (!isAmountEnough) {
                    logger.error("没有足够资金来给用户, inviteRecordId = {}", inviteRecordId);
                    saveFailedRecord(inviteRecordId, IncomeType.INVITE_USER_TASK, "66666", "没有足够资金来给用户，inviteRecordId = " + inviteRecordId, 0L);
                    return;
                }

                String phone = inviteUserRecord.getPhone();

                String txNumber = this.findFailedTxNumber(0L, inviteRecordId, IncomeType.INVITE_USER_TASK, Long.parseLong(phone));

                SpecialActivityCapitalInvestment specialActivityCapitalInvestment = specialActivityCapitalInvestmentApiService.get(capitalInvestmentId);



                String thirdTxNumber = gigoldIncomeByPhone(phone,  specialActivityCapitalInvestment.getOrderNumber(), inviteUserRecord.getAmount(), txNumber, inviteRecordId, IncomeType.INVITE_USER_TASK, 0L);
                if (thirdTxNumber == null) {
                    return;
                }
                specialActivityCapitalInvestmentApiService.plusAmount(capitalInvestmentId, inviteUserRecord.getAmount());
                userTaskRecordApiService.fillInviteDrewTx(inviteRecordId, txNumber);
                this.saveSpecialActivityBillingHistory(txNumber, thirdTxNumber, inviteUserRecord.getAmount(), specialActivityCapitalInvestment);
            }

        } catch (Exception e) {
            logger.error("走账失败");
            saveFailedRecord(inviteRecordId, IncomeType.INVITE_USER_TASK, "88888", "邀请走账失败，inviteRecordId = " + inviteRecordId, 0L);
        } finally {
            redisLock.unlock();
        }
    }

    private String gigoldIncome(String userId, String orderNo, Long amount, String txNumber, Long objectId, IncomeType type, Long advertId) {
        List<Long> idList = Lists.newArrayList();
        idList.add(Long.parseLong(userId));

        String phone = null;
        UserInfo userInfo = userInfoApiService.findUserByUserId(Long.valueOf(userId));
        if (userInfo == null) {
            logger.error("用户信息未找到");
        } else {
            phone = userInfo.getPhone();
        }
        if (StringUtils.isNotEmpty(phone)) {
            return gigoldIncomeByPhone(phone, orderNo, amount, txNumber, objectId, type, advertId);
        } else {
            this.saveFailedRecord(objectId, type, "88888", "查询用户手机号失败", txNumber, advertId);
        }
        return null;
    }

    private String gigoldIncomeByPhone(String phone, String orderNo, Long amount, String txNumber, Long objectId, IncomeType type, Long advertId) {
        RedPacketGetIncomeHttpResponse response;
        try {
            response = gigoldPaymentComponent.redPacketGetIncome(phone, orderNo, amount, txNumber);
        } catch (Exception e) {
            logger.error("调用吉高接口失败 :", e);
            response = new RedPacketGetIncomeHttpResponse();
            response.setRspInf(e.getMessage().length() > PubConstant.gigoldErrorInfoLength ? e.getMessage().substring(0, PubConstant.gigoldErrorInfoLength) : e.getMessage());
            response.setRspCd("99999");
        }

        if (!Constants.GIGOLD_RSPCD_SUCCESS.equals(response.getRspCd())) {
            if (Constants.TXNUMBER_EXISTS.equals(response.getRspCd())) {
                logger.error("营销流水已存在, 已经入账成功, txNumber =  {}", txNumber);
                this.txnumberExists(objectId, type, advertId);
                return response.getRspCd();
            } else {
                this.saveFailedRecord(objectId, type, response.getRspCd(), response.getRspInf(), txNumber, advertId);
            }
        } else {
            this.saveFailedRecord(objectId, type, response.getRspCd(), response.getRspInf(), advertId);
            return response.getResponseJnl();
        }
        return null;
    }

    private void saveFailedRecord(Long objectId, IncomeType type, String resCode, String resInfo, Long advertId) {
        this.saveFailedRecord(objectId, type, resCode, resInfo, "", advertId);
    }

    private void saveFailedRecord(Long objectId, IncomeType type, String resCode, String resInfo, String txNumber, Long advertId) {
        IncomeFailed incomeFailed = incomeFailedApiService.findOne(objectId, type, advertId);
        if (!Constants.GIGOLD_RSPCD_SUCCESS.equals(resCode)) {
            logger.error("极高入账失败, code = {}, msg = {}", resCode, resInfo);
            if (incomeFailed == null) {
                incomeFailed = new IncomeFailed();
                incomeFailed.setRedPacketId(objectId);
                incomeFailed.setType(type);
                incomeFailed.setRspCode(resCode);
                incomeFailed.setRspInfo(resInfo);
                incomeFailed.setTxNumber(txNumber);
                incomeFailed.setAdvertId(advertId);
                incomeFailedApiService.save(incomeFailed);
            } else {
                incomeFailed.setTimes(incomeFailed.getTimes() + 1);
                incomeFailed.setRspCode(resCode);
                incomeFailed.setRspInfo(resInfo);
                if (StringUtils.isNotEmpty(txNumber)) {
                    incomeFailed.setTxNumber(txNumber);
                }
                incomeFailedApiService.update(incomeFailed);
            }
        } else {
            if (incomeFailed != null && incomeFailed.getStatus().equals(YesNoStatus.YES)) {
                incomeFailed.setStatus(YesNoStatus.YES);
                incomeFailed.setTimes(incomeFailed.getTimes() + 1);
                incomeFailedApiService.update(incomeFailed);
            }
        }
    }

    private void txnumberExists(Long objectId, IncomeType type,  Long advertId) {
        IncomeFailed incomeFailed = incomeFailedApiService.findOne(objectId, type, advertId);
        if (incomeFailed != null) {
            incomeFailed.setRspInfo("营销流水已存在");
            incomeFailed.setRspCode(Constants.TXNUMBER_EXISTS);
            incomeFailed.setStatus(YesNoStatus.YES);
            incomeFailed.setTimes(incomeFailed.getTimes() + 1);
            incomeFailedApiService.update(incomeFailed);

        }
    }

    private String findFailedTxNumber(Long advertId, Long objectId, IncomeType incomeType, Long userId) {
        String txNumber = null;
        IncomeFailed incomeFailed = incomeFailedApiService.findOne(objectId, incomeType, advertId);
        if (incomeFailed != null && StringUtils.isNotEmpty(incomeFailed.getTxNumber())) {
            logger.error("存在错误信息, 获取单号, object = {}, incomeType = {}, advertId = {}", objectId, incomeType, advertId);
            txNumber = incomeFailed.getTxNumber();
        } else {
            switch (incomeType) {
                case COLLECT_CARD:
                case RED_PACKET:
                case NEW_HAND_TASK:
                case INVITE_USER_TASK:
                    txNumber = RandomUtil.orderNumberGenerate(userId, DealingsType.DREW);
                    break;
                case PAY:
                case AUDITED_SYNCHRONOUS:
                case AUDIT_FAILED_REFUND:
                {
                    AdvertResponse advertResponse = advertApiService.findOneSimple(advertId);
                    txNumber = advertResponse.getOrderNo();
                    break;
                }
                case OFFLINE_REFUND_CARD:
                case OFFLINE_AMOUNT_PUBLISHED_PRICE_REFUND:
                    txNumber = RandomUtil.orderNumberGenerate(userId, DealingsType.REFUND);
                    break;
                case OFFLINE_MERCHANT_PROFIT_SHARE:
                case OFFLINE_PARTNER_PROFIT_SHARE:
                case OFFLINE_PROPERTY_PROFIT_SHARE:
                    txNumber = RandomUtil.orderNumberGenerate(userId, DealingsType.PROFIT_SHARING);
                    break;
                case SPECIAL_ACTIVITY_REFUND:
                    txNumber = RandomUtil.orderNumberGenerate("001" + userId, DealingsType.PROFIT_SHARING);
                    break;
                case OFFLINE_PLATFORM_PROFIT_SHARE:
                    txNumber = RandomUtil.orderNumberGenerate(sqbjPlatformMerchantNo, DealingsType.PROFIT_SHARING);
                    break;
                default:
                    break;

            }
        }
        return txNumber;
    }

    private void saveSpecialActivityBillingHistory(String txNumber, String thirdTxNumber, Long amount, SpecialActivityCapitalInvestment specialActivityCapitalInvestment) {
        SpecialUserInfo specialUserInfo = specialActivityCapitalInvestmentBillingHistoryApiService.getUser(specialActivityCapitalInvestment.getSpecialUserId());
        SpecialActivityCapitalInvestmentBillingHistory billingHistoryEntity = new SpecialActivityCapitalInvestmentBillingHistory();
        billingHistoryEntity.setCapitalInvestmentId(specialActivityCapitalInvestment.getId());
        billingHistoryEntity.setSpecialUserId(specialActivityCapitalInvestment.getSpecialUserId());
        billingHistoryEntity.setMerchantId(specialUserInfo.getMerchantId());
        billingHistoryEntity.setSucceed(YesNoStatus.YES);
        billingHistoryEntity.setAmount(amount);
        billingHistoryEntity.setDealingsType(cn.lehome.bean.business.ec.enums.admaster.DealingsType.OUTLAY);
        billingHistoryEntity.setTxNumber(txNumber);
        billingHistoryEntity.setOriginTxNumber(specialActivityCapitalInvestment.getOrderNumber());
        billingHistoryEntity.setThirdPartyTxNumber(thirdTxNumber);
        billingHistoryEntity.setProject("用户领取");
        specialActivityCapitalInvestmentBillingHistoryApiService.save(billingHistoryEntity);
    }
}
