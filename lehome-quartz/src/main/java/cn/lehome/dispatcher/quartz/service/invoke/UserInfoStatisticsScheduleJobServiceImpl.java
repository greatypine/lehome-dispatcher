package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.business.activity.bean.activity.ApprenticeContributionStatistics;
import cn.lehome.base.api.business.activity.bean.activity.ContributeSetting;
import cn.lehome.base.api.business.activity.bean.activity.MasterApprenticeRelationship;
import cn.lehome.base.api.business.activity.bean.activity.QContributeSetting;
import cn.lehome.base.api.business.activity.constant.JoinRewardActivityTypeConstants;
import cn.lehome.base.api.business.activity.constant.PubConstant;
import cn.lehome.base.api.business.activity.service.activity.ApprenticeContributionStatisticsApiService;
import cn.lehome.base.api.business.activity.service.activity.ContributeSettingApiService;
import cn.lehome.base.api.business.activity.service.activity.MasterApprenticeRelationshipApiService;
import cn.lehome.base.api.business.activity.service.task.UserTaskOperationRecordApiService;
import cn.lehome.base.api.business.content.bean.comment.QCommentInfoIndex;
import cn.lehome.base.api.business.content.bean.post.QPostInfoIndex;
import cn.lehome.base.api.business.content.bean.record.QOperationRecord;
import cn.lehome.base.api.business.content.bean.silent.UserSilent;
import cn.lehome.base.api.business.content.service.comment.CommentInfoIndexApiService;
import cn.lehome.base.api.business.content.service.post.PostInfoIndexApiService;
import cn.lehome.base.api.business.content.service.silent.UserSilentApiService;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.base.api.common.event.JoinActivityEventBean;
import cn.lehome.base.api.common.util.DateUtil;
import cn.lehome.base.api.user.bean.user.QUserInfoIndex;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.asset.UserBeanFlowApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.business.activity.enums.task.ContributeStatus;
import cn.lehome.bean.business.content.enums.post.PostType;
import cn.lehome.bean.business.content.enums.post.SourceCategory;
import cn.lehome.bean.business.content.enums.post.UserType;
import cn.lehome.bean.user.entity.enums.user.UserStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.*;

@Service("userInfoStatisticsScheduleJobService")
public class UserInfoStatisticsScheduleJobServiceImpl extends AbstractInvokeServiceImpl{

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
    private PostInfoIndexApiService postInfoIndexApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private CommentInfoIndexApiService commentInfoIndexApiService;

    @Autowired
    private UserSilentApiService userSilentApiService;

    @Autowired
    private ApprenticeContributionStatisticsApiService apprenticeContributionStatisticsApiService;

    @Autowired
    private UserBeanFlowApiService userBeanFlowApiService;

    @Autowired
    private UserTaskOperationRecordApiService userTaskOperationRecordApiService;

    @Autowired
    private MasterApprenticeRelationshipApiService masterApprenticeRelationshipApiService;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Autowired
    private ContributeSettingApiService contributeSettingApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        logger.info("开始统计用户信息");

        boolean flag = true;
        int pageSize = 100;
        Long maxUserId = 0L;
        while(flag) {
            try {
                ApiResponse<UserInfoIndex> api = userInfoIndexApiService.findAll(ApiRequest.newInstance().filterLike(QUserInfoIndex.del, UserStatus.NotDeleted).filterGreaterThan(QUserInfoIndex.id, maxUserId), ApiRequestPage.newInstance().paging(0, pageSize).addOrder(QUserInfoIndex.id, PageOrderType.ASC));
                List<UserInfoIndex> userInfoIndexList = Lists.newArrayList(api.getPagedData());
                if (userInfoIndexList != null && userInfoIndexList.size() > 0) {
                    userInfoIndexList.stream().parallel().forEach(userInfoIndex -> {
                        try {
                            ApiRequest squarePost = ApiRequest.newInstance();
                            ApiRequest forumPost = ApiRequest.newInstance();
                            ApiRequest sendHomePage = ApiRequest.newInstance();
                            ApiRequest comment = ApiRequest.newInstance();
                            ApiRequest beReplay = ApiRequest.newInstance();
                            ApiRequest beDeletedPost = ApiRequest.newInstance();
                            ApiRequest beDeletedComment = ApiRequest.newInstance();

                            boolean isChange = false;

                            if (userInfoIndex.getSquarePostNumber() == null) {
                                userInfoIndex.setSquarePostNumber(0L);
                            }
                            if (userInfoIndex.getForumPostNumber() == null) {
                                userInfoIndex.setForumPostNumber(0L);
                            }
                            if (userInfoIndex.getSendHomepageNumber() == null) {
                                userInfoIndex.setSendHomepageNumber(0L);
                            }
                            if (userInfoIndex.getCommentNumber() == null) {
                                userInfoIndex.setCommentNumber(0L);
                            }
                            if (userInfoIndex.getBeReplyNumber() == null) {
                                userInfoIndex.setBeReplyNumber(0L);
                            }
                            if (userInfoIndex.getBeDeletedPostNumber() == null) {
                                userInfoIndex.setBeDeletedPostNumber(0L);
                            }
                            if (userInfoIndex.getBeDeletedCommentNumber() == null) {
                                userInfoIndex.setBeDeletedCommentNumber(0L);
                            }
                            if (userInfoIndex.getBeSilentNumber() == null) {
                                userInfoIndex.setBeSilentNumber(0L);
                            }

                            squarePost.filterEqual(QPostInfoIndex.userId, userInfoIndex.getId()).filterLike(QPostInfoIndex.postType, PostType.CRN).filterLike(QPostInfoIndex.userType, UserType.APP);
                            Long squarePostNumber = postInfoIndexApiService.countFindAll(squarePost);

                            forumPost.filterEqual(QPostInfoIndex.userId, userInfoIndex.getId()).filterLike(QPostInfoIndex.postType, PostType.LT).filterLike(QPostInfoIndex.userType, UserType.APP);
                            Long forumPostNumber = postInfoIndexApiService.countFindAll(forumPost);

                            sendHomePage.filterEqual(QPostInfoIndex.userId, userInfoIndex.getId()).filterLike(QPostInfoIndex.pushIndex, YesNoStatus.YES).filterLike(QPostInfoIndex.userType, UserType.APP);
                            Long sendHomePageNumber = postInfoIndexApiService.countFindAll(sendHomePage);

                            comment.filterEqual(QCommentInfoIndex.commentUserId, userInfoIndex.getId()).filterLike(QCommentInfoIndex.commentUserType, UserType.APP);
                            Long commentNumber = commentInfoIndexApiService.countFindAll(comment);

                            beReplay.filterEqual(QCommentInfoIndex.beReplyUserId, userInfoIndex.getId()).filterLike(QCommentInfoIndex.beReplyUserType, UserType.APP);
                            Long beReplyNumber = commentInfoIndexApiService.countFindAll(beReplay);

                            beDeletedPost.filterEqual(QOperationRecord.sourceUserId, userInfoIndex.getId()).filterEqual(QOperationRecord.isSelfOperator, YesNoStatus.NO).filterEqual(QOperationRecord.sourceCategory, SourceCategory.POST).filterEqual(QOperationRecord.operatorUserId, 0);
                            Long beDeletedPostNumber = postInfoIndexApiService.countFindAll(beDeletedPost);

                            beDeletedComment.filterEqual(QOperationRecord.sourceUserId, userInfoIndex.getId()).filterEqual(QOperationRecord.isSelfOperator, YesNoStatus.NO).filterEqual(QOperationRecord.sourceCategory, SourceCategory.COMMENT).filterEqual(QOperationRecord.operatorUserId, 0);
                            Long beDeletedCommentNumber = commentInfoIndexApiService.countFindAll(beDeletedComment);

                            UserSilent userSilent = userSilentApiService.findByUserId(userInfoIndex.getId());
                            Long beSilentNumber = new Long(0);
                            if (userSilent != null) {
                                beSilentNumber = userSilent.getSilentTimes().longValue();
                            }
                            if (!(userInfoIndex.getBeSilentNumber().equals(beSilentNumber))) {
                                isChange = true;
                            }

                            if (!(squarePostNumber.equals(userInfoIndex.getSquarePostNumber()) && forumPostNumber.equals(userInfoIndex.getForumPostNumber()) && sendHomePageNumber.equals(userInfoIndex.getSendHomepageNumber())
                                    && commentNumber.equals(userInfoIndex.getCommentNumber()) && beReplyNumber.equals(userInfoIndex.getBeReplyNumber()) && beDeletedPostNumber.equals(userInfoIndex.getBeDeletedPostNumber())
                                    && beDeletedCommentNumber.equals(userInfoIndex.getBeDeletedCommentNumber()))) {
                                isChange = true;
                            }
                            if (isChange) {
                                userInfoIndex.setSquarePostNumber(squarePostNumber);
                                userInfoIndex.setForumPostNumber(forumPostNumber);
                                userInfoIndex.setSendHomepageNumber(sendHomePageNumber);
                                userInfoIndex.setCommentNumber(commentNumber);
                                userInfoIndex.setBeReplyNumber(beReplyNumber);
                                userInfoIndex.setBeDeletedPostNumber(beDeletedPostNumber);
                                userInfoIndex.setBeDeletedCommentNumber(beDeletedCommentNumber);
                                userInfoIndex.setBeSilentNumber(beSilentNumber);
                                userInfoIndexApiService.updateUserStatisticsInfo(userInfoIndex);
                            }
                        } catch (Exception e) {
                            logger.error("统计用户论坛数据：" + userInfoIndex.getId() + "时出错", e);
                        }
                        try {
                            //更新用户进贡统计数据
                            this.updateApprenticeContribution(userInfoIndex.getId());
                        }catch(Exception e){
                            e.printStackTrace();
                            logger.error("统计用户进贡数据：" + userInfoIndex.getId() + "时出错", e);
                        }

                    });
                    maxUserId = userInfoIndexList.get(userInfoIndexList.size() - 1).getId();
                } else {
                    flag = false;
                }
            }catch(Exception e){
                logger.error("查询用户数据时出错",e);
            }
        }

    }

    private void updateApprenticeContribution(Long userId){
        ApprenticeContributionStatistics apprenticeContributionStatistics = apprenticeContributionStatisticsApiService.findOneByUserId(userId);
        if(apprenticeContributionStatistics==null){
            apprenticeContributionStatistics = new ApprenticeContributionStatistics();
            apprenticeContributionStatistics.setUserId(userId);
            apprenticeContributionStatistics.setTotalContributionBean(0l);
            apprenticeContributionStatistics.setContributionTimes(0l);
            apprenticeContributionStatistics.setContributionMoney(0l);
        }else if(simpleDateFormat.format(apprenticeContributionStatistics.getUpdatedTime()).equals(simpleDateFormat.format(new Date()))){
            logger.warn("数据已经统计，不需要再重复统计");
            return;
        }

        //通过徒弟找师傅信息
        MasterApprenticeRelationship masterApprenticeRelationship = masterApprenticeRelationshipApiService.findOneByApprenticeId(userId);
        //今天获取的豆总数，算减的，不计徒弟进贡的豆
        Date start = this.getYesterdayTime(new Date(),true);
        Date end = this.getYesterdayTime(new Date(),false);
        Long subBeanNum = userBeanFlowApiService.findSubBeanCountByUserId(userId, PubConstant.subOperationTypeList,start,end);
        Long rewardBeanNum = userTaskOperationRecordApiService.findBeanNum(userId, PubConstant.contributionOperationTypeList,start,end);
        Long beanCount = rewardBeanNum - subBeanNum;
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
            logger.error("未找到最新的进贡梯度设置");
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
        calendar.add(Calendar.DAY_OF_MONTH, -1);
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
