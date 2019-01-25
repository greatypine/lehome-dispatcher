package cn.lehome.dispatcher.utils.contribution;

import cn.lehome.base.api.advertising.bean.activity.ApprenticeContributionStatistics;
import cn.lehome.base.api.advertising.bean.activity.ContributeSetting;
import cn.lehome.base.api.advertising.bean.activity.MasterApprenticeRelationship;
import cn.lehome.base.api.advertising.bean.activity.QContributeSetting;
import cn.lehome.base.api.advertising.constant.JoinRewardActivityTypeConstants;
import cn.lehome.base.api.advertising.constant.PubConstant;
import cn.lehome.base.api.advertising.service.activity.ApprenticeContributionStatisticsApiService;
import cn.lehome.base.api.advertising.service.activity.ContributeSettingApiService;
import cn.lehome.base.api.advertising.service.activity.MasterApprenticeRelationshipApiService;
import cn.lehome.base.api.advertising.service.task.UserTaskOperationRecordApiService;
import cn.lehome.base.api.tool.compoment.jms.EventBusComponent;
import cn.lehome.base.api.tool.constant.EventConstants;
import cn.lehome.base.api.tool.event.JoinActivityEventBean;
import cn.lehome.base.api.tool.util.DateUtil;
import cn.lehome.base.api.user.bean.user.UserInfo;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.asset.UserBeanFlowApiService;
import cn.lehome.base.api.user.service.user.UserInfoApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.advertising.enums.task.ContributeStatus;
import cn.lehome.bean.user.search.user.UserInfoIndexEntity;
import cn.lehome.dispatcher.utils.es.util.EsFlushUtil;
import cn.lehome.dispatcher.utils.es.util.EsScrollResponse;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by zuoguodong on 2018/6/1
 */
@Service("contributionService")
public class ContributionServiceImpl implements ContributionService{

    @Autowired
    private ApprenticeContributionStatisticsApiService apprenticeContributionStatisticsApiService;

    @Autowired
    private UserBeanFlowApiService userBeanFlowApiService;

    @Autowired
    private UserTaskOperationRecordApiService userTaskOperationRecordApiService;

    @Autowired
    private MasterApprenticeRelationshipApiService masterApprenticeRelationshipApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Autowired
    private ContributeSettingApiService contributeSettingApiService;

    @Autowired
    private UserInfoApiService userInfoApiService;

    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private static final Map<Long,Long> CONTRIBUTION_TIMES_MONEY_MAP= new HashMap();
    static{
        CONTRIBUTION_TIMES_MONEY_MAP.put(1l,100l);
        CONTRIBUTION_TIMES_MONEY_MAP.put(2l,100l);
        CONTRIBUTION_TIMES_MONEY_MAP.put(3l,100l);
        CONTRIBUTION_TIMES_MONEY_MAP.put(4l,100l);
        CONTRIBUTION_TIMES_MONEY_MAP.put(5l,100l);
        CONTRIBUTION_TIMES_MONEY_MAP.put(6l,100l);
    }

    /**
     * 3.2.1升级以后使用的Map 每次奖励2块钱 分3次给完6块
     */
    private static final Map<Long,Long> CONTRIBUTION_TIMES_MONEY_MAP_V2= new HashMap();
    static{
        CONTRIBUTION_TIMES_MONEY_MAP_V2.put(1l,200l);
        CONTRIBUTION_TIMES_MONEY_MAP_V2.put(2l,200l);
        CONTRIBUTION_TIMES_MONEY_MAP_V2.put(3l,200l);
    }

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    @Override
    public void contribution(String[] params) {
        if(params.length < 2){
            System.out.println("参数错误");
            return;
        }
        Date date = null;
        try {
            date = simpleDateFormat.parse(params[1]);
        } catch (ParseException e) {
            System.out.println("日期输入错误");
            return;
        }
        if (params.length >= 3) {
            for(int i = 2;i<params.length;i++){
                this.contribution(date,Long.valueOf(params[i]));
            }
        } else {
            final Date execDate = date;
            int pageNo = 0;
            int size = 100;

            List<UserInfo> userInfoList = userInfoApiService.findAll(pageNo, size);
            while (userInfoList.size() != 0) {
                final List<UserInfo> list = userInfoList;
                threadPoolExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        list.forEach(u -> {
                            try {
                                contribution(execDate, u.getUserId());
                            } catch (Exception e) {
                                System.out.println(u.getUserId());
                                e.printStackTrace();
                            }
                        });
                    }
                });
                pageNo++;
                userInfoList = userInfoApiService.findAll(pageNo, size);
                System.out.println("pageNo:" + pageNo);
            }
            while (threadPoolExecutor.getQueue().size() != 0) {
                try {
                    System.out.println("用户数据加载完毕，还有" + threadPoolExecutor.getQueue().size() + "任务等执行");
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("数据处理完毕");
    }

    private void contribution(Date date,Long userId){
        System.out.println("userId["+ userId + "] date[" + simpleDateFormat.format(date)+"] ");
        ApprenticeContributionStatistics apprenticeContributionStatistics = apprenticeContributionStatisticsApiService.findOneByUserId(userId);
        if(apprenticeContributionStatistics==null){
            apprenticeContributionStatistics = new ApprenticeContributionStatistics();
            apprenticeContributionStatistics.setUserId(userId);
            apprenticeContributionStatistics.setTotalContributionBean(0l);
            apprenticeContributionStatistics.setContributionTimes(0l);
            apprenticeContributionStatistics.setContributionMoney(0l);
            apprenticeContributionStatistics.setPlanContributionMoney(0L);
            apprenticeContributionStatistics.setContributeStatus(ContributeStatus.DEFAULT);
        }
        //通过徒弟找师傅信息
        MasterApprenticeRelationship masterApprenticeRelationship = masterApprenticeRelationshipApiService.findOneByApprenticeId(userId);
        //今天获取的豆总数，算减的，不计徒弟进贡的豆
        Date start = this.getYesterdayTime(date,true);
        Date end = this.getYesterdayTime(date,false);
        Long subBeanNum = userBeanFlowApiService.findSubBeanCountByUserId(userId,PubConstant.subOperationTypeList,start,end);
        Long rewardBeanNum = userTaskOperationRecordApiService.findBeanNum(userId, PubConstant.contributionOperationTypeList,start,end);
        Long beanCount = rewardBeanNum - subBeanNum;
        System.out.println("userId[" + userId + "] TodayObtainBean[" + beanCount + "] ");
        apprenticeContributionStatistics.setTodayObtainBean(beanCount);
        //今天进贡的豆
        Long todayContributionBean = 0l;
        //进贡次数
        Long contributionTimes = apprenticeContributionStatistics.getContributionTimes();
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userId);
        // 用户被系统认定为模拟器登录注册后，被管理员审核通过，则给师傅钱
        if (ContributeStatus.AUDITED.equals(apprenticeContributionStatistics.getContributeStatus())) {
            Long planContributionMoney = apprenticeContributionStatistics.getPlanContributionMoney();
            apprenticeContributionStatistics.setContributionMoney(apprenticeContributionStatistics.getContributionMoney() + planContributionMoney);
            apprenticeContributionStatistics.setPlanContributionMoney(0L);
            apprenticeContributionStatistics.setContributeStatus(ContributeStatus.DEFAULT);

            sendContributionMoneyMsg(masterApprenticeRelationship.getMasterId(), userId, planContributionMoney);
        }

        // 获取进贡梯度设置信息
        ContributeSetting lastestContributeSetting = contributeSettingApiService.findLastestOne();
        if (lastestContributeSetting == null) {
            System.out.println("未找到最新的进贡梯度设置");
            return;
        }
        if (masterApprenticeRelationship != null && DateUtil.getDiffSeconds(lastestContributeSetting.getStartTime(), masterApprenticeRelationship.getCreatedTime()) > 0) {
            ApiRequest apiRequest = ApiRequest.newInstance();
            apiRequest.filterLessEqual(QContributeSetting.startTime, masterApprenticeRelationship.getCreatedTime());
            apiRequest.filterGreaterThan(QContributeSetting.endTime, masterApprenticeRelationship.getCreatedTime());
            List<ContributeSetting> settingList = contributeSettingApiService.findAll(apiRequest);
            if (!CollectionUtils.isEmpty(settingList)) {
                lastestContributeSetting = settingList.get(0);
            }
        }

        String[] moneyGapArray = lastestContributeSetting.getBonus().split(",");
        if(beanCount >= 10){
            todayContributionBean = beanCount / 10;
            contributionTimes++;
            //如果有师傅信息，给师傅加豆
            if(masterApprenticeRelationship!=null) {
                sendContributionBeanMsg(masterApprenticeRelationship.getMasterId(),userId, todayContributionBean);
                System.out.println("userId[" + userId + "] sendContributionBeanMsg, masterId [" + masterApprenticeRelationship.getMasterId() + "]");
                if(beanCount >= 30) {
                    //进贡钱数
                    if (contributionTimes <= moneyGapArray.length) {
                        Long contributionMoney = Long.valueOf(moneyGapArray[contributionTimes.intValue() - 1]);
                        if (contributionMoney != null) {
                            //6次以内，给师傅加钱
                            // 20180710 新增规则：师傅每天邀请到十个人以后，只进贡不给钱
                            if (YesNoStatus.YES.equals(masterApprenticeRelationship.getIfGainMoney())) {
                                if (YesNoStatus.YES.equals(userInfoIndex.getIsSimulator()) ) {
                                    apprenticeContributionStatistics.setPlanContributionMoney(apprenticeContributionStatistics.getPlanContributionMoney() + contributionMoney);
                                    apprenticeContributionStatistics.setContributeStatus(ContributeStatus.AUDIT);
                                } else {
                                    apprenticeContributionStatistics.setContributionMoney(apprenticeContributionStatistics.getContributionMoney() + contributionMoney);
                                    sendContributionMoneyMsg(masterApprenticeRelationship.getMasterId(), userId, contributionMoney);
                                    System.out.println("userId[" + userId + "] sendContributionMoneyMsg, masterId [" + masterApprenticeRelationship.getMasterId() + "]");
                                }
                            }
                        }
                    }
                }
                //总共进贡的豆
                apprenticeContributionStatistics.setTotalContributionBean(apprenticeContributionStatistics.getTotalContributionBean()+todayContributionBean);
                apprenticeContributionStatistics.setContributionTimes(contributionTimes);
            }
        }
        apprenticeContributionStatistics.setTodayContributionBean(todayContributionBean);

        //最后活跃时间
        Date lastActivityTime = userTaskOperationRecordApiService.findLastActivityTime(userId,PubConstant.contributionOperationTypeList);
        apprenticeContributionStatistics.setLastActiveTime(lastActivityTime);
        //邀请时间
        if(apprenticeContributionStatistics.getInvitedTime() == null){
            if(masterApprenticeRelationship!=null) {
                Date invitedTime = masterApprenticeRelationship.getCreatedTime();
                apprenticeContributionStatistics.setInvitedTime(invitedTime);
            }
        }
        apprenticeContributionStatisticsApiService.save(apprenticeContributionStatistics);
        System.out.println("userId[" + userId + "] end");
    }

    /**
     * 获取前一天的日期
     * @param date
     * @param isStart
     * @return
     */
    public Date getYesterdayTime(Date date,boolean isStart){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        if (isStart) {
            calendar.set(Calendar.HOUR_OF_DAY,0);
            calendar.set(Calendar.MINUTE,0);
            calendar.set(Calendar.SECOND,0);
            calendar.set(Calendar.MILLISECOND,0);
            return calendar.getTime();
        }
        calendar.set(Calendar.HOUR_OF_DAY,23);
        calendar.set(Calendar.MINUTE,59);
        calendar.set(Calendar.SECOND,59);
        calendar.set(Calendar.MILLISECOND,999);
        return calendar.getTime();
    }

    private void sendContributionBeanMsg(Long userId,Long apprenticeId,Long todayContributionBean){
        JoinActivityEventBean joinActivityEventBean = new JoinActivityEventBean();
        joinActivityEventBean.setJoinActivityType(JoinRewardActivityTypeConstants.SUBORDINATE_CONTRIBUTION);
        List<Object> attributes = Lists.newArrayList();
        attributes.add(userId);
        attributes.add(apprenticeId);
        attributes.add(todayContributionBean);
        joinActivityEventBean.setAttributes(attributes);
        eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.JOIN_REWARD_ACTIVITY_EVENT, joinActivityEventBean));
    }

    private void sendContributionMoneyMsg(Long userId,Long apprenticeId,Long todayContributionMoney){
        JoinActivityEventBean joinActivityEventBean = new JoinActivityEventBean();
        joinActivityEventBean.setJoinActivityType(JoinRewardActivityTypeConstants.INVITE_FRIEND_V2);
        List<Object> attributes = Lists.newArrayList();
        attributes.add(userId);
        attributes.add(apprenticeId);
        attributes.add(todayContributionMoney);
        joinActivityEventBean.setAttributes(attributes);
        eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.JOIN_REWARD_ACTIVITY_EVENT, joinActivityEventBean));
    }
}
