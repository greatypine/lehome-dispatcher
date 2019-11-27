package cn.lehome.dispatcher.queue.service.impl.entrance;

import cn.lehome.base.api.acs.bean.region.Park;
import cn.lehome.base.api.acs.bean.region.QRegion;
import cn.lehome.base.api.acs.bean.region.Region;
import cn.lehome.base.api.acs.bean.user.User;
import cn.lehome.base.api.acs.service.region.ParkApiService;
import cn.lehome.base.api.acs.service.region.RegionApiService;
import cn.lehome.base.api.acs.service.user.UserApiService;
import cn.lehome.base.api.common.business.oauth2.bean.user.Oauth2AccountIndex;
import cn.lehome.base.api.common.business.oauth2.bean.user.QOauth2AccountIndex;
import cn.lehome.base.api.common.business.oauth2.bean.user.QUserAccountAreaIndex;
import cn.lehome.base.api.common.business.oauth2.bean.user.UserAccountAreaIndex;
import cn.lehome.base.api.common.business.oauth2.service.user.UserAccountIndexApiService;
import cn.lehome.base.pro.api.bean.households.HouseholdIndex;
import cn.lehome.base.pro.api.bean.households.QHouseholdIndex;
import cn.lehome.base.pro.api.bean.regions.ControlRegions;
import cn.lehome.base.pro.api.bean.regions.ControlRegionsPositionRelationship;
import cn.lehome.base.pro.api.bean.regions.QControlRegions;
import cn.lehome.base.pro.api.bean.regions.QControlRegionsPositionRelationship;
import cn.lehome.base.pro.api.service.households.HouseholdIndexApiService;
import cn.lehome.base.pro.api.service.regions.ControlRegionsApiService;
import cn.lehome.bean.acs.enums.user.UserType;
import cn.lehome.bean.pro.enums.regions.ControlRegionType;
import cn.lehome.dispatcher.queue.service.entrance.AutoEntranceService;
import cn.lehome.dispatcher.queue.service.impl.AbstractBaseServiceImpl;
import cn.lehome.framework.base.api.core.compoment.loader.LoaderServiceComponent;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2019/6/21.
 */
@Service
public class AutoEntranceServiceImpl extends AbstractBaseServiceImpl implements AutoEntranceService {

    @Autowired
    private UserApiService userApiService;

    @Autowired
    private ParkApiService parkApiService;

    @Autowired
    private RegionApiService regionApiService;

    @Autowired
    private ControlRegionsApiService controlRegionsApiService;

    @Autowired
    private LoaderServiceComponent loaderServiceComponent;

    @Autowired
    private HouseholdIndexApiService householdIndexApiService;

    @Autowired
    private UserAccountIndexApiService businessUserAccountIndexApiService;

    @Override
    public User getUserByHousehold(HouseholdIndex householdIndex) {
        User user = userApiService.findByTraceId(UserType.Resident, householdIndex.getOpenId());
        if (user == null) {
            user = new User();
            user.setTraceId(householdIndex.getOpenId());
            user.setUserType(UserType.Resident);
            user.setUserId(householdIndex.getUserId() == null ? 0L : householdIndex.getUserId().longValue());
            user = userApiService.create(user);
        } else {
            if (householdIndex.getUserId() != null && householdIndex.getUserId() != 0L) {
                userApiService.updateUserId(user.getId(), Long.valueOf(householdIndex.getUserId()));
            }
        }
        return user;
    }

    @Override
    public User getUserByAccount(Oauth2AccountIndex oauth2AccountIndex) {
        User user = null;
        if (oauth2AccountIndex.getMutiOpenId() != null) {
            for (String openId : oauth2AccountIndex.getMutiOpenId()) {
                user = userApiService.findByTraceId(UserType.StaffMember, openId);
                if (user != null) {
                    logger.error("原始有开门用户 ：" + JSON.toJSONString(user));
                    break;
                }
            }
        } else {
            user = userApiService.findByTraceId(UserType.StaffMember, oauth2AccountIndex.getUserOpenId());
        }
        if (user == null) {
            logger.error("创建员工开门用户 ：" + oauth2AccountIndex.getAccountId());
            user = new User();
            user.setTraceId(oauth2AccountIndex.getUserOpenId());
            user.setUserType(UserType.StaffMember);
            user.setUserId(Long.valueOf(oauth2AccountIndex.getAccountId()));
            user = userApiService.create(user);
        } else {
            logger.error("绑定员工ID开门用户 ：" + oauth2AccountIndex.getAccountId());
            userApiService.updateUserId(user.getId(), Long.valueOf(oauth2AccountIndex.getAccountId()));
        }
        return user;
    }

    @Override
    public void modifyUserRegion(User user, List<HouseholdIndex> householdIndices) {
        Park park = parkApiService.findByTraceId(householdIndices.get(0).getAreaId().toString());
        if (park == null) {
            return;
        }
        Long areaId = householdIndices.get(0).getAreaId();
        Set<Integer> managerAreaIds = householdIndices.stream().map(HouseholdIndex::getManageAreaId).filter(managerAreaId -> managerAreaId != null && managerAreaId != 0).collect(Collectors.toSet());
        Set<Integer> floorIds = householdIndices.stream().map(HouseholdIndex::getFloorId).filter(floorId -> floorId != null && floorId != 0).collect(Collectors.toSet());
        Set<Integer> unitIds = householdIndices.stream().map(HouseholdIndex::getUnitId).filter(unitId -> unitId != null && unitId != 0).collect(Collectors.toSet());
        List<ControlRegions> controlRegionses = Lists.newArrayList();
        List<ControlRegions> areaControlRegionses = controlRegionsApiService.findByPosition(ApiRequest.newInstance().filterEqual(QControlRegionsPositionRelationship.communityId, areaId));
        if (!CollectionUtils.isEmpty(areaControlRegionses)) {
            loaderServiceComponent.loadBatch(areaControlRegionses, ControlRegions.class, QControlRegions.positionRelationships);
            for (ControlRegions controlRegions : areaControlRegionses) {
                if (controlRegions.getControlRegionType().equals(ControlRegionType.COMMUNITY) && controlRegions.getDeleted().equals(YesNoStatus.NO)) {
                    controlRegionses.add(controlRegions);
                } else if (controlRegions.getControlRegionType().equals(ControlRegionType.PROJECT) && controlRegions.getDeleted().equals(YesNoStatus.NO)) {
                    if (!CollectionUtils.isEmpty(managerAreaIds)) {
                        for (ControlRegionsPositionRelationship relationship : controlRegions.getPositionRelationships()) {
                            if (managerAreaIds.contains(relationship.getProjectId().intValue())) {
                                controlRegionses.add(controlRegions);
                                break;
                            }
                        }
                    }
                } else if (controlRegions.getControlRegionType().equals(ControlRegionType.FLOOR) && controlRegions.getDeleted().equals(YesNoStatus.NO)) {
                    if (!CollectionUtils.isEmpty(floorIds)) {
                        for (ControlRegionsPositionRelationship relationship : controlRegions.getPositionRelationships()) {
                            if (floorIds.contains(relationship.getFloorId().intValue())) {
                                controlRegionses.add(controlRegions);
                                break;
                            }
                        }
                    }
                } else {
                    if (!CollectionUtils.isEmpty(unitIds)) {
                        for (ControlRegionsPositionRelationship relationship : controlRegions.getPositionRelationships()) {
                            if (unitIds.contains(relationship.getUnitId().intValue())) {
                                controlRegionses.add(controlRegions);
                                break;
                            }
                        }
                    }
                }
            }
        }

        if (!CollectionUtils.isEmpty(controlRegionses)) {
            List<Region> regions = regionApiService.findAll(ApiRequest.newInstance().filterIn(QRegion.traceId, controlRegionses.stream().map(ControlRegions::getId).collect(Collectors.toList())));
            regions = regions.stream().filter(region -> region.getAutoAccept()).collect(Collectors.toList());
            regionApiService.modifyUserRegion(user.getId(), park.getId(), regions.stream().map(Region::getId).collect(Collectors.toList()));
        }
    }

    @Override
    public void deleteAllUserRegion(User user, Long areaId) {
        Park park = parkApiService.findByTraceId(areaId.toString());
        if (park == null) {
            return;
        }
        regionApiService.deleteAllUserRegion(user.getId(), park.getId());
    }

    @Override
    public void modifyUserRegionByArea(User user, Long areaId) {
        Park park = parkApiService.findByTraceId(areaId.toString());
        if (park == null) {
            return;
        }
        if (!regionApiService.userHasParkRegion(user.getId(), areaId)) {
            List<ControlRegions> controlRegionses = controlRegionsApiService.findByPosition(ApiRequest.newInstance().filterEqual(QControlRegionsPositionRelationship.communityId, areaId));
            if (!CollectionUtils.isEmpty(controlRegionses)) {
                controlRegionses = controlRegionses.stream().filter(controlRegions -> controlRegions.getDeleted().equals(YesNoStatus.NO)).collect(Collectors.toList());
            }
            if (!CollectionUtils.isEmpty(controlRegionses)) {
                List<Region> regionList = regionApiService.findAll(ApiRequest.newInstance().filterIn(QRegion.traceId, controlRegionses.stream().map(ControlRegions::getId).collect(Collectors.toList())));
                regionList = regionList.stream().filter(region -> region.getAutoAccept()).collect(Collectors.toList());
                if (!CollectionUtils.isEmpty(regionList)) {
                    regionApiService.modifyUserRegion(user.getId(), park.getId(), regionList.stream().map(Region::getId).collect(Collectors.toList()));
                }
            }
        }
    }

    @Override
    public List<User> loadAllUser(ControlRegions controlRegions) {
        List<User> users = Lists.newArrayList();
        List<HouseholdIndex> householdIndexList = Lists.newArrayList();
        if (controlRegions.getControlRegionType().equals(ControlRegionType.COMMUNITY)) {
            householdIndexList = householdIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseholdIndex.areaId, controlRegions.getPositionRelationships().get(0).getCommunityId()));
        } else if (controlRegions.getControlRegionType().equals(ControlRegionType.PROJECT)) {
            List<Integer> managerIds = controlRegions.getPositionRelationships().stream().map(ControlRegionsPositionRelationship::getProjectId).map(projectId -> projectId.intValue()).collect(Collectors.toList());
            householdIndexList = householdIndexApiService.findAll(ApiRequest.newInstance().filterIn(QHouseholdIndex.manageAreaId, managerIds));
        } else if (controlRegions.getControlRegionType().equals(ControlRegionType.FLOOR)) {
            List<Integer> floorIds = controlRegions.getPositionRelationships().stream().map(ControlRegionsPositionRelationship::getFloorId).map(floorId -> floorId.intValue()).collect(Collectors.toList());
            householdIndexList = householdIndexApiService.findAll(ApiRequest.newInstance().filterIn(QHouseholdIndex.floorId, floorIds));
        } else {
            List<Integer> unitIds = controlRegions.getPositionRelationships().stream().map(ControlRegionsPositionRelationship::getUnitId).map(unitId -> unitId.intValue()).collect(Collectors.toList());
            householdIndexList = householdIndexApiService.findAll(ApiRequest.newInstance().filterIn(QHouseholdIndex.unitId, unitIds));
        }
        if (!CollectionUtils.isEmpty(householdIndexList)) {
            users.addAll(householdIndexList.stream().map(householdIndex -> this.getUserByHousehold(householdIndex)).collect(Collectors.toList()));
        }
        List<UserAccountAreaIndex> userAreaAccounts = businessUserAccountIndexApiService.findAreaAll(ApiRequest.newInstance().filterEqual(QUserAccountAreaIndex.areaId, controlRegions.getPositionRelationships().get(0).getCommunityId()));
        if (!CollectionUtils.isEmpty(userAreaAccounts)) {
            List<Oauth2AccountIndex> oauth2AccountIndexList = businessUserAccountIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QOauth2AccountIndex.clientId, "sqbj-smart").filterIn(QOauth2AccountIndex.accountId, userAreaAccounts.stream().map(UserAccountAreaIndex::getAccountId).collect(Collectors.toList())));
            users.addAll(oauth2AccountIndexList.stream().map(oauth2AccountIndex -> this.getUserByAccount(oauth2AccountIndex)).collect(Collectors.toList()));
        }
        return users;
    }
}
