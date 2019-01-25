package cn.lehome.dispatcher.utils.user;

import cn.lehome.base.api.advertising.bean.activity.UserAddiationalIndex;
import cn.lehome.base.api.advertising.service.activity.MasterApprenticeRelationshipApiService;
import cn.lehome.base.api.advertising.service.activity.UserAddiationalIndexApiService;
import cn.lehome.base.api.bigdata.service.actionlog.ActionLogStatisticApiService;
import cn.lehome.base.api.business.service.exchange.BeansExchangeApplyRecordApiService;
import cn.lehome.base.api.tool.bean.community.Community;
import cn.lehome.base.api.tool.bean.device.ClientDeviceIndex;
import cn.lehome.base.api.tool.bean.device.PushDeviceInfo;
import cn.lehome.base.api.tool.bean.job.ScheduleJob;
import cn.lehome.base.api.tool.service.community.CommunityCacheApiService;
import cn.lehome.base.api.tool.service.device.ClientDeviceIndexApiService;
import cn.lehome.base.api.tool.service.device.DeviceApiService;
import cn.lehome.base.api.tool.service.job.ScheduleJobApiService;
import cn.lehome.base.api.user.bean.asset.UserAssetInfo;
import cn.lehome.base.api.user.bean.user.*;
import cn.lehome.base.api.user.service.asset.UserAssetApiService;
import cn.lehome.base.api.user.service.user.UserCommunityRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.actionlog.bean.UserDeviceBean;
import cn.lehome.bean.actionlog.bean.UserHouseRelationshipBean;
import cn.lehome.bean.actionlog.bean.UserInfoBean;
import cn.lehome.bean.tool.entity.enums.device.PushVendorType;
import cn.lehome.bean.user.entity.enums.user.HouseType;
import cn.lehome.bean.user.search.user.UserInfoIndexEntity;
import cn.lehome.dispatcher.utils.es.util.EsFlushUtil;
import cn.lehome.dispatcher.utils.es.util.EsScrollResponse;
import cn.lehome.dispatcher.utils.util.ZookeeperUtils;
import cn.lehome.framework.actionlog.core.ActionLogRequest;
import cn.lehome.framework.actionlog.core.bean.AppActionLog;
import cn.lehome.framework.actionlog.core.enums.HbaseDataType;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import cn.lehome.framework.base.api.core.util.ExcelUtils;
import cn.lehome.framework.base.api.core.util.StringUtil;
import cn.lehome.framework.bean.core.enums.ClientType;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import cn.lehome.framework.constant.ActionKeyConstants;
import cn.lehome.framework.constant.HttpLogConstants;
import cn.lehome.framework.constant.UserActionKeyConstants;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.File;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Created by zuoguodong on 2018/3/22
 */
@Service
public class UserServiceImpl implements UserService {

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private UserInfoApiService userInfoApiService;

    @Autowired
    private UserAssetApiService userAssetApiService;

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService;

    @Autowired
    private ScheduleJobApiService scheduleJobApiService;

    @Autowired
    private MasterApprenticeRelationshipApiService masterApprenticeRelationshipApiService;

    @Autowired
    private BeansExchangeApplyRecordApiService beansExchangeApplyRecordApiService;

    @Autowired
    private UserAddiationalIndexApiService userAddiationalIndexApiService;

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private ActionLogStatisticApiService actionLogStatisticApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private UserCommunityRelationshipApiService userCommunityRelationshipApiService;

    @Autowired
    private ClientDeviceIndexApiService clientDeviceIndexApiService;

    @Autowired
    private DeviceApiService deviceApiService;

    @Autowired
    private ActionLogRequest actionLogRequest;

    @Value("${dubbo.registry.address}")
    private String zookeeperConnectStr;

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void refreshUserCache(String[] input) {
        int pageNo = 0;
        if (input.length > 1) {
            try {
                pageNo = Integer.valueOf(input[1]);
                System.out.println();
            } catch (Exception e) {
                pageNo = 0;
            }
        }
        int size = 100;

        List<UserInfo> userInfoList = userInfoApiService.findAll(pageNo, size);
        while (userInfoList.size() != 0) {
            final List<UserInfo> list = userInfoList;
            threadPoolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    list.forEach(u -> {
                        try {
                            userInfoIndexApiService.saveOrUpdateBase(u);
                            userInfoIndexApiService.updateUserAuthCommunity(u.getUserId());
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
        System.out.println("数据处理完毕");
    }

    @Override
    public void initUserAdditionalIndex(String[] input) {
        if (input.length != 2) {
            System.out.println("输入参数有误，请重新输入");
            return;
        }
        Integer pageSize = Integer.valueOf(input[1]);
        long millis = 30000L;
        Integer execUserNums = 0;
        EsScrollResponse scrollResponse = EsFlushUtil.getInstance().searchByScroll(UserInfoIndexEntity.class, pageSize, null, millis);
        if (CollectionUtils.isEmpty(scrollResponse.getDatas())) {
            System.out.println("无可处理数据！");
            return;
        }
        setUserAdditionalInfo(scrollResponse.getDatas());
        EsFlushUtil.getInstance().batchUpdate(scrollResponse.getDatas());
        execUserNums = scrollResponse.getDatas().size();
        String scrollId = scrollResponse.getScrollId();
        while (true) {
            EsScrollResponse esScrollResponse = EsFlushUtil.getInstance().searchByScrollId(UserInfoIndexEntity.class, pageSize, scrollId, millis);
            if (esScrollResponse == null || CollectionUtils.isEmpty(esScrollResponse.getDatas())) {
                break;
            }
            setUserAdditionalInfo(esScrollResponse.getDatas());
            EsFlushUtil.getInstance().batchUpdate(esScrollResponse.getDatas());
            scrollId = esScrollResponse.getScrollId();
            execUserNums += esScrollResponse.getDatas().size();
            System.out.println("已处理用户数 num=" + execUserNums);
        }
        System.out.println("数据处理完成！");
    }


    @Override
    public void saveUserToHbase() {

        //zookeeper开关打开
        ZookeeperUtils.updateZookeeperWatcher(zookeeperConnectStr, ActionKeyConstants.SYNC_USER_TO_HBASE_ZOOKEEPER_SWITCH, "on");

        ZSetOperations<String, String> zset = redisTemplate.opsForZSet();
        SetOperations<String, String> set = redisTemplate.opsForSet();

        AtomicLong i = new AtomicLong();
        Long prevUserId = 0L;
        while (true) {
            long j = i.getAndIncrement();
            //每次取一个记录
            Set<String> sets = zset.range(ActionKeyConstants.SYNC_USER_TO_HBASE_REDIS_KEY, j, j);
            if (CollectionUtils.isEmpty(sets)) {

                ApiRequest apiRequest = ApiRequest.newInstance();
                if (prevUserId > 0L) {
                    apiRequest.filterGreaterThan(QUserInfoIndex.id, prevUserId);
                }
                ApiRequestPage page = ApiRequestPage.newInstance();
                page.addOrder(QUserInfoIndex.id, PageOrderType.ASC);
                //每次取一个用户
                page.paging(0, 1);

                ApiResponse<UserInfoIndex> users = userInfoIndexApiService.findAll(apiRequest, page);

                Collection<UserInfoIndex> pagedData = users.getPagedData();
                if (CollectionUtils.isEmpty(pagedData)) {
                    break;
                }

                UserInfoIndex user = Lists.newArrayList(pagedData).get(0);
                logger.info("current deal user:" + user.getId());

                UserInfoBean userInfoBean = BeanMapping.map(user, UserInfoBean.class);
                String clientId = StringUtil.defaultIfBlank(user.getClientId(), user.getLastClientId());
                if (StringUtils.isBlank(clientId)) {
                    prevUserId = user.getId();
                    logger.info("user[" + user.getId() + "] clientId is null");
                    continue;
                }
                ClientDeviceIndex clientDeviceIndex = clientDeviceIndexApiService.get(clientId, ClientType.SQBJ);
                if (Objects.isNull(clientDeviceIndex)) {
                    prevUserId = user.getId();
                    logger.info("user[" + user.getId() + "] clientDeviceIndex is null");
                    continue;
                }
                // region 同步用户基本信息
                this.actionLogStatisticApiService.syncUserToHbase(userInfoBean);

                List<String> verifyData = Lists.newArrayList();
                // endregion
                String verifyData0 = HbaseDataType.USER + "-" + userInfoBean.getId();
                verifyData.add(verifyData0);

                // region 同步用户设备信息
                String verifyData1 = this.syncUserDeviceToHbase(user, clientDeviceIndex);
                verifyData.add(verifyData1);
                // endregion

                // region 同步用户房产信息
                List<String> verifyData2 = this.syncUserHouseToHbase(user);
                verifyData.addAll(verifyData2);
                // endregion
                //set.add(ActionKeyConstants.SYNC_DATA_TO_HBASE_REDIS_KEY, verifyData.toArray(new String[verifyData.size()]));
                //zset.add(ActionKeyConstants.SYNC_USER_TO_HBASE_REDIS_KEY, user.getUserOpenId(), j);
                prevUserId = user.getId();
            } else {
                prevUserId = Long.parseLong(Lists.newArrayList(sets).get(0));
                logger.info("jump user:" + prevUserId);
            }
        }
        //zookeeper开关关闭
        ZookeeperUtils.updateZookeeperWatcher(zookeeperConnectStr, ActionKeyConstants.SYNC_USER_TO_HBASE_ZOOKEEPER_SWITCH, "off");
    }

    private List<UserInfoIndexEntity> setUserAdditionalInfo(List<UserInfoIndexEntity> entities) {
        List<Long> userIds = entities.stream().filter(entity -> entity.getId() != null).map(UserInfoIndexEntity::getId).collect(Collectors.toList());
        List<UserAddiationalIndex> addiationalIndexs = userAddiationalIndexApiService.findAll(userIds);
        Map<Long, UserAddiationalIndex> addiationalIndexMap = addiationalIndexs.stream().collect(Collectors.toMap(UserAddiationalIndex::getUserId, e -> e));
        for (UserInfoIndexEntity entity : entities) {
            if (entity.getIsSimulator() == null) {
                entity.setIsSimulator(YesNoStatus.NO);
            }
            UserAddiationalIndex addiationalIndex = addiationalIndexMap.get(entity.getId());
            if (addiationalIndex == null) {
                entity.setExchangeFrozenTimes(0);
                entity.setIfBeInvitedUser(false);
                entity.setApprenticeNum(0L);
                continue;
            }
            entity.setIfBeInvitedUser(YesNoStatus.YES.equals(addiationalIndex.getIfBeInvited()) ? true : false);
            entity.setExchangeFrozenTimes(addiationalIndex.getExchangeFrozenTimes());
            entity.setApprenticeNum(addiationalIndex.getApprenticeNum().longValue());
        }
        return entities;
    }

    @Override
    public void repairUserIndexWithDB(String[] input) {
        if (input.length < 2) {
            System.out.println("输入参数有误，请重新输入");
            return;
        }
        if (input.length == 3) {
            findDiffAndUpdate(Long.valueOf(input[2]));
            System.out.println("数据处理完成！");
            return;
        }
        Integer pageSize = Integer.valueOf(input[1]);
        long millis = 30000L;
        Integer execUserNums = 0;
        EsScrollResponse scrollResponse = EsFlushUtil.getInstance().searchByScroll(UserInfoIndexEntity.class, pageSize, null, millis);
        if (CollectionUtils.isEmpty(scrollResponse.getDatas())) {
            System.out.println("无可处理数据！");
            return;
        }
        findDiffAndUpdate(scrollResponse.getDatas());
        EsFlushUtil.getInstance().batchUpdate(scrollResponse.getDatas());
        execUserNums = scrollResponse.getDatas().size();
        String scrollId = scrollResponse.getScrollId();
        while (true) {
            EsScrollResponse esScrollResponse = EsFlushUtil.getInstance().searchByScrollId(UserInfoIndexEntity.class, pageSize, scrollId, millis);
            if (esScrollResponse == null || CollectionUtils.isEmpty(esScrollResponse.getDatas())) {
                break;
            }
            findDiffAndUpdate(esScrollResponse.getDatas());
            EsFlushUtil.getInstance().batchUpdate(esScrollResponse.getDatas());
            scrollId = esScrollResponse.getScrollId();
            execUserNums += esScrollResponse.getDatas().size();
            System.out.println("已处理用户数 num=" + execUserNums);
        }
        System.out.println("数据处理完成！");
    }

    @Override
    public void updateWeChatIndex(String[] input) {

    }

    private void findDiffAndUpdate(Long userId) {
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userId);
        Long apprenticeNumIndex = userInfoIndex.getApprenticeNum() == null ? 0L : userInfoIndex.getApprenticeNum();
        Long apprenticeNumDB = masterApprenticeRelationshipApiService.countByMasterId(userInfoIndex.getId());
        if (apprenticeNumDB.compareTo(apprenticeNumIndex) != 0) {
            userInfoIndexApiService.updateUserApprenticeNum(userInfoIndex.getId(), apprenticeNumDB.intValue() - apprenticeNumIndex.intValue());
        }
    }

    private List<UserInfoIndexEntity> findDiffAndUpdate(List<UserInfoIndexEntity> entities) {
        for (UserInfoIndexEntity entity : entities) {
            Long apprenticeNumIndex = entity.getApprenticeNum() == null ? 0L : entity.getApprenticeNum();
            Long apprenticeNumDB = masterApprenticeRelationshipApiService.countByMasterId(entity.getId());
            if (apprenticeNumDB.compareTo(apprenticeNumIndex) != 0) {
                entity.setApprenticeNum(apprenticeNumDB);
            }
        }
        return entities;
    }

    @Override
    public void refreshUserCacheByPhone(String[] input) {
        if (input.length < 2) {
            System.out.println("参数错误");
            return;
        }
        for (int i = 1; i < input.length; i++) {
            UserInfo userInfo = userInfoApiService.findByPhone(input[i]);
            try {
                userInfoIndexApiService.saveOrUpdateBase(userInfo);
                userInfoIndexApiService.updateUserAuthCommunity(userInfo.getUserId());
            } catch (Exception e) {
                System.out.println(userInfo.getUserId() + ":" + e.getMessage());
                e.printStackTrace();
            }
        }
        System.out.println("数据处理完毕");
    }

    @Override
    public void kickUser(String[] input) {
        if (input.length < 2) {
            System.out.println("参数错误");
            return;
        }
        AtomicInteger count = new AtomicInteger();
        String filePath = input[1];
        ExcelUtils excelUtils = new ExcelUtils(filePath);
        List<List<String>> list = excelUtils.read();
        list.forEach(l -> {
            try {
                count.getAndIncrement();
                Long relationId = Long.valueOf(l.get(0));
                userHouseRelationshipApiService.kickUser(relationId);

                // region 2018年09月18日 用户房产相关埋点
                UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.get(relationId);
                UserInfoIndex userInfo = userInfoIndexApiService.findByUserId(userHouseRelationship.getUserId());
                String clientId = StringUtil.defaultIfBlank(userInfo.getClientId(), userInfo.getLastClientId());
                AppActionLog.Builder builder = AppActionLog.newBuilder(userInfo.getUserOpenId(), clientId);
                builder.setActionKey(UserActionKeyConstants.USER_HOUSE_REMOVE);
                builder.addMap(HttpLogConstants.COMMUNITY_EXT_ID, userHouseRelationship.getCommunityExtId());
                List<Long> communityIds = communityCacheApiService.getCommunityExtBind(userHouseRelationship.getCommunityExtId());
                if (com.alibaba.dubbo.common.utils.CollectionUtils.isNotEmpty(communityIds)) {
                    Community community = communityCacheApiService.getCommunity(communityIds.get(0));
                    if (Objects.nonNull(community)) {
                        builder.addMap(HttpLogConstants.COMMUNITY_ID, community.getId());
                    }
                }
                ClientDeviceIndex clientDeviceIndex = clientDeviceIndexApiService.get(clientId, ClientType.SQBJ);
                if (Objects.nonNull(clientDeviceIndex)) {
                    builder.addMap(HttpLogConstants.VERSION, clientDeviceIndex.getAppVersion());
                }
                builder.send(actionLogRequest);
                // endregion

            } catch (Exception e) {
                System.out.println(l.get(0) + " : " + e.getMessage());
                e.printStackTrace();
            }
        });


        System.out.println("数据处理完毕: " + count);
    }

    @Override
    public void repairUserCache() {
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
                            List<UserHouseRelationship> list = userHouseRelationshipApiService.findByUserId(u.getUserId());
                            UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(u.getUserId());
                            if (compare(list, userInfoIndex)) {
                                System.out.println(" userId[" + u.getUserId() + "],dbInfo[" + getDbUserInfo(list)
                                        + "],esInfo[" + getEsUserInfo(userInfoIndex) + "]");
                                userInfoIndexApiService.saveOrUpdateBase(u);
                                userInfoIndexApiService.updateUserAuthCommunity(u.getUserId());
                            }

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
                System.out.println(" repariUserCache 用户数据加载完毕，还有" + threadPoolExecutor.getQueue().size() + "任务等执行");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("数据处理完毕");
    }

    @Override
    public void userInformationStatistics() {

        String cronExpression = String.format("%s %s %s %s %s %s", 0, 0, 0, "*", "*", "?");
        ScheduleJob scheduleJob = new ScheduleJob();
        scheduleJob.setIsSync(YesNoStatus.YES);
        scheduleJob.setIsOnce(YesNoStatus.NO);
        scheduleJob.setJobGroup("userInfo-statistics-group");
        scheduleJob.setJobName("userInfo-statistics-job");
        scheduleJob.setAliasName("用户信息统计");
        scheduleJob.setCronExpression(cronExpression);
        scheduleJob.setDescription("用户信息统计");
        scheduleJob.setExeParams("");
        scheduleJob.setExeServiceName("userInfoStatisticsScheduleJobService");
        scheduleJobApiService.create(scheduleJob);
    }

    private boolean compare(List<UserHouseRelationship> list, UserInfoIndex userInfoIndex) {
        if (CollectionUtils.isEmpty(list)) {
            return false;
        }
        if (userInfoIndex == null) {
            return true;
        }

        for (UserHouseRelationship userHouseRelationship : list) {
            if (!userHouseRelationship.getHouseType().equals(HouseType.FORBID)
                    && userHouseRelationship.getEnableStatus().equals(EnableDisableStatus.ENABLE)) {
                if (CollectionUtils.isEmpty(userInfoIndex.getAuthHouseIds())) {
                    return true;
                }
                if (CollectionUtils.isEmpty(userInfoIndex.getAuthHouseIds())) {
                    return true;
                }
                if (!userInfoIndex.getAuthHouseIds().contains(userHouseRelationship.getId())) {
                    return true;
                }
                if (!userInfoIndex.getAuthCommunityIds().contains(userHouseRelationship.getCommunityExtId())) {
                    return true;
                }
            }
        }
        return false;
    }

    private String getDbUserInfo(List<UserHouseRelationship> list) {
        StringBuffer houseSb = new StringBuffer("houseId:");
        StringBuffer communitySb = new StringBuffer("communityId:");
        list.forEach(userHouseRelationship -> {
            houseSb.append(userHouseRelationship.getId()).append(",");
            communitySb.append(userHouseRelationship.getCommunityExtId()).append(",");
        });
        return houseSb.append(" | ").append(communitySb).toString();
    }

    private String getEsUserInfo(UserInfoIndex userInfoIndex) {
        StringBuffer houseSb = new StringBuffer("houseId:");
        StringBuffer communitySb = new StringBuffer("communityId:");
        userInfoIndex.getAuthHouseIds().forEach(houseId -> houseSb.append(houseId).append(","));
        userInfoIndex.getAuthCommunityIds().forEach(communityId -> communitySb.append(communityId).append(","));
        return houseSb.append(" | ").append(communitySb).toString();
    }

    private String syncUserDeviceToHbase(UserInfoIndex user, ClientDeviceIndex clientDevice) {

        UserDeviceBean bean = BeanMapping.map(clientDevice, UserDeviceBean.class);

        PushDeviceInfo pushDeviceInfo = deviceApiService.getPushDeviceInfo(clientDevice.getId(), PushVendorType.JIGUANG);
        bean.setDeviceToken(pushDeviceInfo.getDeviceToken());

        actionLogStatisticApiService.syncUserDeviceToHbase(bean);

        return HbaseDataType.DEVICE.toString() + "-" + bean.getId();
    }

    private List<String> syncUserHouseToHbase(UserInfoIndex user) {

        List<UserCommunityRelationship> ucList = userCommunityRelationshipApiService.findByUserId(user.getId());
        List<UserHouseRelationship> uhList = userHouseRelationshipApiService.findByUserId(user.getId());

        List<Long> uhExtIds = Lists.newArrayList();
        List<UserHouseRelationshipBean> uhs = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(uhList)) {
            uhExtIds = uhList.stream().map(p -> p.getCommunityExtId()).collect(Collectors.toList());
        }

        //通过UserCommunity拿到communityId 然后再拿到社区所对应的ExtId 然后再查询UserHouse中是否存在 不存在则构建一个空数据 写入到Hbase 存在则略过（去重）
        if (!CollectionUtils.isEmpty(ucList)) {
            for (UserCommunityRelationship uc : ucList) {
                Long communityId = uc.getCommunityId();
                Community community = communityCacheApiService.getCommunity(communityId);
                Long communityExtId = community.getCommunityExtId();
                if (!uhExtIds.contains(communityExtId)) {

                    UserHouseRelationshipBean uh = new UserHouseRelationshipBean();
                    uh.setCommunityExtId(communityExtId);
                    uh.setCommunityId(community.getId());
                    uh.setCreatedTime(community.getCreatedTime());
                    uh.setUpdatedTime(community.getUpdatedTime());
                    uh.setUserId(user.getId());
                    uh.setOpenId(user.getUserOpenId());
                    uh.setId(community.getId());
                    uh.setProvinceCode(community.getPcode());
                    uh.setCityCode(community.getCitycode());
                    uh.setAreaCode(community.getAdcode());
                    uh.setAuthCommunity(false);
                    uh.setAvailable(true);
                    uhs.add(uh);
                }
            }
        }
        //组织已认证房产数据
        if (!CollectionUtils.isEmpty(uhList)) {
            for (UserHouseRelationship uh : uhList) {

                UserHouseRelationshipBean uhBean = new UserHouseRelationshipBean();
                uhBean.setHouseId(uh.getHouseId());
                uhBean.setHouseType(uh.getHouseType().toString());
                uhBean.setRemark(uh.getRemark());
                uhBean.setEnableStatus(uh.getEnableStatus().toString());
                uhBean.setOperatorId(uh.getOperatorId());
                uhBean.setCommunityExtId(uh.getCommunityExtId());
                uhBean.setCreatedTime(uh.getCreatedTime());
                uhBean.setUpdatedTime(uh.getUpdatedTime());
                uhBean.setUserId(user.getId());
                uhBean.setOpenId(user.getUserOpenId());
                uhBean.setId(uh.getId());


                List<Long> communityIds = communityCacheApiService.getCommunityExtBind(uh.getCommunityExtId());
                if (!CollectionUtils.isEmpty(communityIds)) {
                    Community community = communityCacheApiService.getCommunity(communityIds.get(0));
                    if (Objects.nonNull(community)) {

                        uhBean.setCommunityId(community.getId());
                        uhBean.setProvinceCode(community.getPcode());
                        uhBean.setCityCode(community.getCitycode());
                        uhBean.setAreaCode(community.getAdcode());
                    }
                }

                uhBean.setAuthCommunity(true);
                uhBean.setAvailable(true);

                uhs.add(uhBean);
            }
        }

        if (!CollectionUtils.isEmpty(uhs)) {
            actionLogStatisticApiService.syncUserHouseToHbase(uhs);
        }

        return uhs.stream().map(p -> HbaseDataType.HOUSE + "-" + p.getId()).collect(Collectors.toList());
    }

    @Override
    public void updateUserRegisterSourceIndex(String[] input) {
        if (input.length < 2) {
            System.out.println("输入参数有误，请重新输入");
            return;
        }
        if (input.length == 3) {
            findUserIndexAndUpdate(Long.valueOf(input[2]));
            System.out.println("数据处理完成！");
            return;
        }
        Integer pageSize = Integer.valueOf(input[1]);
        long millis = 30000L;
        Integer execUserNums = 0;
        EsScrollResponse scrollResponse = EsFlushUtil.getInstance().searchByScroll(UserInfoIndexEntity.class, pageSize, null, millis);
        if (CollectionUtils.isEmpty(scrollResponse.getDatas())) {
            System.out.println("无可处理数据！");
            return;
        }
        findUserIndexAndUpdate(scrollResponse.getDatas());
        EsFlushUtil.getInstance().batchUpdate(scrollResponse.getDatas());
        execUserNums = scrollResponse.getDatas().size();
        String scrollId = scrollResponse.getScrollId();
        while (true) {
            EsScrollResponse esScrollResponse = EsFlushUtil.getInstance().searchByScrollId(UserInfoIndexEntity.class, pageSize, scrollId, millis);
            if (esScrollResponse == null || CollectionUtils.isEmpty(esScrollResponse.getDatas())) {
                break;
            }
            findUserIndexAndUpdate(esScrollResponse.getDatas());
            EsFlushUtil.getInstance().batchUpdate(esScrollResponse.getDatas());
            scrollId = esScrollResponse.getScrollId();
            execUserNums += esScrollResponse.getDatas().size();
            System.out.println("已处理用户数 num=" + execUserNums);
        }
        System.out.println("数据处理完成！");
    }

    @Override
    public void updateUserIndexFromExcel(String[] input) {
        String filePath = input[1];
        int sheetIndex = 0;
        if (input.length == 3) {
            sheetIndex = Integer.valueOf(input[2]);
        }
        File file = new File(filePath);
        if (!file.isFile()) {
            System.out.println("文件路径不对，非法输入");
        }

        ExcelUtils excelUtils = new ExcelUtils(file.getAbsolutePath());
        List<List<String>> list = excelUtils.read(sheetIndex);
        list.stream().forEach( obj -> {
            UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(Long.valueOf(obj.get(0)));
            if (userInfoIndex != null) {
                UserAssetInfo userAssetInfo = userAssetApiService.findByUserId(Long.valueOf(obj.get(0)));
                if (userAssetInfo != null) {
                    userInfoIndexApiService.saveOrUpdateUserAsset(userAssetInfo);
                    System.out.println("用户资产已刷新, userId=" + userInfoIndex.getId());
                }
            }
        });
        System.out.println("用户资产刷新完成");
    }

    private List<UserInfoIndexEntity> findUserIndexAndUpdate(List<UserInfoIndexEntity> entities){
        List<Long> userIds = entities.stream().filter(entity -> entity.getId() != null).map(UserInfoIndexEntity::getId).collect(Collectors.toList());
        Map<Long, UserInfo> userInfoMap = userInfoApiService.findAll(userIds);
        for (UserInfoIndexEntity entity : entities) {
            UserInfo userInfo = userInfoMap.get(entity.getId());
            if (userInfo != null) {
                entity.setRegisterType(userInfo.getRegisterType());
            }
        }
        return entities;
    }

    private void findUserIndexAndUpdate(Long userId){
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userId);
        if (userInfoIndex != null) {
            UserInfo userInfo = userInfoApiService.findUserByUserId(userId);
            if (userInfo != null) {
                userInfoIndexApiService.updateUserRegisterType(userId,userInfo.getRegisterType());
            }
        }
    }

}
