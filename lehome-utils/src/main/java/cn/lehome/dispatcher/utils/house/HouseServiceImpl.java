package cn.lehome.dispatcher.utils.house;

import cn.lehome.base.api.property.bean.house.HouseDetailInfo;
import cn.lehome.base.api.property.service.house.HouseInfoApiService;
import cn.lehome.base.api.tool.bean.community.Community;
import cn.lehome.base.api.tool.bean.community.CommunityExt;
import cn.lehome.base.api.tool.service.community.CommunityApiService;
import cn.lehome.base.api.tool.service.community.CommunityCacheApiService;
import cn.lehome.base.api.tool.service.device.ClientDeviceIndexApiService;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.framework.actionlog.core.ActionLogRequest;
import cn.lehome.framework.actionlog.core.bean.AppActionLog;
import cn.lehome.framework.constant.UserActionKeyConstants;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by zuoguodong on 2018/3/21
 */
@Service
public class HouseServiceImpl implements HouseService {

    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private CommunityApiService communityApiService;

    @Autowired
    private HouseInfoApiService houseInfoApiService;
    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    @Autowired
    private ClientDeviceIndexApiService clientDeviceIndexApiService;

    @Autowired
    private ActionLogRequest actionLogRequest;

    @Override
    public void repairHouseAddress(String[] args) {
        if (args.length < 2) {
            System.out.println("token不能为空");
        }
        if (args.length == 2) {
            repairHouseAddressBatch(args);
        }
        if (args.length > 2) {
            repairHouseAddressByIds(args);
        }

    }

    private void repairHouseAddressBatch(String args[]) {
        int pageNo = 0;
        int pageSize = 100;
        String token = args[1];
        //获取数据
        List<UserHouseRelationship> userHouseRelationshipList = userHouseRelationshipApiService.findAll(pageNo, pageSize);
        while (userHouseRelationshipList.size() != 0) {
            final List<UserHouseRelationship> list = userHouseRelationshipList;
            threadPoolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    list.forEach(u -> {
                        try {
                            repairOne(u, token);
                        } catch (Exception e) {
                            System.out.println(u.getId());
                            e.printStackTrace();
                        }
                    });
                }
            });
            pageNo++;
            userHouseRelationshipList = userHouseRelationshipApiService.findAll(pageNo, pageSize);
            System.out.println("pageNo:" + pageNo);
        }
        while (threadPoolExecutor.getQueue().size() != 0) {
            try {
                System.out.println("repairHouseAddressBatch数据加载完毕，还有" + threadPoolExecutor.getQueue().size() + "任务等执行");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("数据处理完毕");
    }

    private void repairHouseAddressByIds(String args[]) {
        int count = 0;
        int detailCount = 0;
        String token = args[1];
        for (int i = 2; i < args.length; i++) {
            count++;
            try {
                Long id = Long.valueOf(args[i]);
                UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.get(id);
                this.repairOne(userHouseRelationship, token);
                detailCount++;
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
        }
        System.out.println("数据处理完毕,需要处理数据：" + count + "处理数据：" + detailCount);
    }

    public void repairOne(UserHouseRelationship u, String token) {
        try {

            List<Long> communityIds = communityCacheApiService.getCommunityExtBind(u.getCommunityExtId());
            String addressPrefix = "";
            if (!CollectionUtils.isEmpty(communityIds)) {
                Community community = communityApiService.get(communityIds.get(0));
                addressPrefix = community.getFullAddress();
            }
            if (null == u.getHouseAddress() || "".equals(u.getHouseAddress())) {
                CommunityExt communityExt = communityCacheApiService.getCommunityExt(u.getCommunityExtId());
                String editionType = communityExt.getEditionType().toString();
                Map<String, String> header = new HashMap<>();
                header.put("API-APP-ID", "prop-android-debug");
                header.put("API-PRO-APP-OAUTH-TOKEN", token);
                header.put("API-Client-ID", "zyx-client-id");
                header.put("Edition-Type", editionType);
                HouseDetailInfo houseDetailInfo = houseInfoApiService.findOne(u.getHouseId(), header);
                u.setHouseAddress(houseDetailInfo.getAddress());
            }
            u.setFullHouseAddress(addressPrefix + u.getHouseAddress());
            userHouseRelationshipApiService.updateUserAddress(u);

            // region 2018年09月18日 用户房产相关埋点
            UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.get(u.getId());
            UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userHouseRelationship.getUserId());
            AppActionLog.newBuilder(UserActionKeyConstants.USER_HOUSE_UPDATE, userInfoIndex.getUserOpenId(), StringUtils.defaultIfBlank(userInfoIndex.getClientId(), userInfoIndex.getLastClientId())).send(actionLogRequest);
            // endregion

        } catch (Exception e) {
            throw e;
        }
    }


//    public static void main(String[] args) {
//        long testId = 14L;
//        UserHouseRelationship addUserHouseRelationship = new UserHouseRelationship();
//        addUserHouseRelationship.setHouseType(HouseType.MAIN);
//        addUserHouseRelationship.setCommunityExtId(testId);
//        addUserHouseRelationship.setHouseId(testId);
//        addUserHouseRelationship.setRemark("");
//        addUserHouseRelationship.setFamilyMemberName("");
//        addUserHouseRelationship.setFullHouseAddress("");
//        addUserHouseRelationship.setHouseAddress("");
//        addUserHouseRelationship.setUserId(6L);
//
//        ActionLogRequest.send(ActionLog.newBuilder().addActionLogBean(UserActionKeyConstants.USER_HOUSE_UPDATE, System.currentTimeMillis(), "abcd", 0L, map -> map.put(UserActionKeyConstants.Properties.USER_HOUSE_RELATIONSHIP, JSON.toJSONString(addUserHouseRelationship))).build());
//    }


}
