package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.business.activity.bean.task.DailyConversionAccount;
import cn.lehome.base.api.business.activity.bean.task.RateSettingInfo;
import cn.lehome.base.api.business.activity.service.task.DailyConversionAccountApiService;
import cn.lehome.base.api.business.activity.service.task.RateSettingApiService;
import cn.lehome.base.api.business.activity.service.task.UserTaskOperationRecordApiService;
import cn.lehome.base.api.business.activity.service.wechat.WeChatTaskOperationRecordApiService;
import cn.lehome.base.api.user.service.asset.UserBeanFlowApiService;
import cn.lehome.base.api.user.service.asset.UserDepositFlowApiService;
import cn.lehome.bean.business.activity.entity.task.DailyConversionAccountEntity;
import cn.lehome.bean.business.activity.enums.task.AssetType;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.bean.core.enums.Operation;
import cn.lehome.framework.bean.core.enums.OperationType;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author yanwenkai
 * @date 2018/5/22
 */
@Service("dailyConversionAccountJobService")
public class DailyConversionAccountJobServiceImpl extends AbstractInvokeServiceImpl{

    public final static String IS_START = "1";

    @Autowired
    private UserTaskOperationRecordApiService userTaskOperationRecordApiService;

    @Autowired
    private UserBeanFlowApiService userBeanFlowApiService;

    @Autowired
    private RateSettingApiService rateSettingApiService;

    @Autowired
    private DailyConversionAccountApiService dailyConversionAccountApiService;

    @Autowired
    private UserDepositFlowApiService userDepositFlowApiService;

    @Autowired
    private WeChatTaskOperationRecordApiService weChatTaskOperationRecordApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        logger.info("定时统计账单流水开始。。。。。。。");
        DailyConversionAccountEntity entity = new DailyConversionAccountEntity();
        Calendar calendar = Calendar.getInstance();
        Date date = calendar.getTime();
        Date start = getYesterdayTime(date, IS_START,-1);
        Date end = getYesterdayTime(date, null,-1);
        Long sumBean = userTaskOperationRecordApiService.findBeanSumBySend(Operation.ADD, AssetType.BEAN, start, end);
        if (sumBean == null) {
            sumBean = 0L;
        }
        sumBean += getWeChatSumBean(start,end);
        entity.setBeanNum(sumBean);
        Long conversionBean = userBeanFlowApiService.findOperationNum(OperationType.BEAN_CONVERT_MONEY,Operation.SUB,start,end);
        if (conversionBean == null) {
            conversionBean = 0L;
        }
        entity.setConversionBean(conversionBean);
        Long conversionMoney = userDepositFlowApiService.findOperationNum(OperationType.BEAN_CONVERT_MONEY,Operation.ADD,start,end);
        if (conversionMoney == null) {
            conversionMoney = 0L;
        }
        entity.setConversionMoney(conversionMoney);
        RateSettingInfo rateSettingInfo = rateSettingApiService.getRateSettingInfo();
        Integer amt = rateSettingInfo.getAmt();
        double v = (double) amt / 100;
        Integer beanNum = rateSettingInfo.getBeanNum();
        Long consumeBean = userTaskOperationRecordApiService.findBeanSumByConsume(getOperationTypeList(),Operation.SUB,AssetType.BEAN,start,end);
        if (consumeBean == null) {
            consumeBean = 0L;
        }
        entity.setBeanConsumeNum(consumeBean);
        Long beanNetIncome = sumBean - consumeBean;
        entity.setBeanNetIncome(beanNetIncome);

        Date before2DayStart = getYesterdayTime(date, IS_START, -2);
        Date before2DayEnd = getYesterdayTime(date, null, -2);
        DailyConversionAccount account = dailyConversionAccountApiService.findOneByDate(before2DayStart,before2DayEnd);
        Long inviteRewardByDay = userTaskOperationRecordApiService.sumInviteReward(OperationType.INVITE_FRIEND_V2,start,end);
        if (inviteRewardByDay == null) {
            inviteRewardByDay = 0L;
        }
        if (account != null) {
            account.setInviteRewardByDay(inviteRewardByDay);
            dailyConversionAccountApiService.update(account);
        }
        String dailyLastRate = beanNum + "/" + v;
        entity.setDailyLastRate(getDailyLastRate(dailyLastRate,conversionBean,conversionMoney));
        entity.setCreatedTime(start);
        dailyConversionAccountApiService.createRecord(entity);
        logger.info("定时统计账单流水完成。。。");
    }

    /**
     * 统计小程序内产生金豆数量
     * @param start
     * @param end
     * @return
     */
    private Long getWeChatSumBean(Date start,Date end){
        Long sumBean = weChatTaskOperationRecordApiService.findBeanSumBySend(Operation.ADD, AssetType.BEAN, start, end, YesNoStatus.NO);
        if (sumBean == null) {
            sumBean = 0L;
        }
        logger.info("定时统计小程序未绑定手机号产生的金豆数，sumBean:{}",sumBean);
        return sumBean;
    }

    private static String getDailyLastRate(String dailyLastRate,Long conversionBean, Long conversionMoney) {
        if (conversionBean > 0 && conversionMoney > 0) {
            double v = (double) conversionBean / conversionMoney;
            DecimalFormat df = new DecimalFormat("0.0");
            return df.format(v) +"/0.01";
        }
        return dailyLastRate;
    }

    public List<OperationType> getOperationTypeList(){
        List<OperationType> list = Lists.newArrayList();
        list.add(OperationType.DRAW_CARD);
        list.add(OperationType.DEL_POST_MANAGER);
        list.add(OperationType.DEL_POST_SELF);
        list.add(OperationType.STEAL_CARD);
        return list;
    }

    /**
     * 获取前n天的日期
     * @param date
     * @param isStart
     * @param amount
     * @return
     */
    public Date getYesterdayTime(Date date,String isStart,Integer amount){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, amount);
        if (IS_START.equals(isStart)) {
            calendar.set(Calendar.HOUR_OF_DAY,0);
            calendar.set(Calendar.SECOND,0);
            calendar.set(Calendar.MINUTE,0);
            calendar.set(Calendar.MILLISECOND,0);
            return calendar.getTime();
        }
        calendar.set(Calendar.HOUR_OF_DAY,23);
        calendar.set(Calendar.MINUTE,59);
        calendar.set(Calendar.SECOND,59);
        calendar.set(Calendar.MILLISECOND,999);
        return calendar.getTime();
    }
}
