package cn.lehome.dispatcher.queue.listener.areaImport;

import cn.lehome.base.api.common.bean.community.CommunityExt;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.base.api.common.custom.oauth2.bean.user.UserAccount;
import cn.lehome.base.api.common.custom.oauth2.service.user.UserAccountApiService;
import cn.lehome.base.api.common.service.community.CommunityApiService;
import cn.lehome.base.api.old.pro.bean.area.OldManagerArea;
import cn.lehome.base.api.old.pro.bean.area.QOldManagerArea;
import cn.lehome.base.api.old.pro.bean.house.*;
import cn.lehome.base.api.old.pro.bean.household.OldHouseholdsInfo;
import cn.lehome.base.api.old.pro.bean.household.OldHouseholdsSettingsInfo;
import cn.lehome.base.api.old.pro.service.area.OldManagerAreaApiService;
import cn.lehome.base.api.old.pro.service.house.OldFloorInfoApiService;
import cn.lehome.base.api.old.pro.service.house.OldFloorUnitInfoApiService;
import cn.lehome.base.api.old.pro.service.house.OldHouseInfoApiService;
import cn.lehome.base.api.old.pro.service.household.OldHouseholdsInfoApiService;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.base.pro.api.bean.address.AddressBaseInfo;
import cn.lehome.base.pro.api.bean.address.QAddressBaseInfo;
import cn.lehome.base.pro.api.bean.area.*;
import cn.lehome.base.pro.api.bean.event.ImportDataPartConstants;
import cn.lehome.base.pro.api.bean.event.ImportEventBean;
import cn.lehome.base.pro.api.bean.house.*;
import cn.lehome.base.pro.api.bean.households.HouseholdCertification;
import cn.lehome.base.pro.api.bean.households.HouseholdIndex;
import cn.lehome.base.pro.api.bean.households.QHouseholdIndex;
import cn.lehome.base.pro.api.bean.households.settings.Household;
import cn.lehome.base.pro.api.bean.households.settings.QHouseholdsSettingsInfo;
import cn.lehome.base.pro.api.service.address.AddressBaseApiService;
import cn.lehome.base.pro.api.service.area.AreaInfoApiService;
import cn.lehome.base.pro.api.service.area.ImportIdMapperApiService;
import cn.lehome.base.pro.api.service.area.ImportTaskApiService;
import cn.lehome.base.pro.api.service.area.ManagerAreaApiService;
import cn.lehome.base.pro.api.service.house.*;
import cn.lehome.base.pro.api.service.households.HouseholdCertificationApiService;
import cn.lehome.base.pro.api.service.households.HouseholdIndexApiService;
import cn.lehome.base.pro.api.service.households.HouseholdsInfoApiService;
import cn.lehome.bean.pro.enums.*;
import cn.lehome.bean.pro.enums.address.ExtendType;
import cn.lehome.bean.pro.enums.area.ImportTaskStatus;
import cn.lehome.bean.pro.enums.house.*;
import cn.lehome.bean.pro.enums.household.ApprovalStatus;
import cn.lehome.bean.pro.enums.household.CertifiedClientType;
import cn.lehome.bean.pro.enums.household.CertifiedSourceType;
import cn.lehome.bean.pro.enums.household.IdentifyType;
import cn.lehome.bean.pro.old.enums.RecordDeleteStatus;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import cn.lehome.framework.bean.core.enums.SexType;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by wuzhao on 2019/5/31.
 */
public class AreaImportListener extends AbstractJobListener {

    @Autowired
    private ImportTaskApiService importTaskApiService;

    @Autowired
    private OldManagerAreaApiService oldManagerAreaApiService;

    @Autowired
    private OldFloorInfoApiService oldFloorInfoApiService;

    @Autowired
    private OldFloorUnitInfoApiService oldFloorUnitInfoApiService;

    @Autowired
    private ImportIdMapperApiService importIdMapperApiService;

    @Autowired
    private ManagerAreaApiService managerAreaApiService;

    @Autowired
    private FloorInfoApiService smartFloorInfoApiService;

    @Autowired
    private FloorUnitInfoApiService floorUnitInfoApiService;

    @Autowired
    private AreaInfoApiService smartAreaInfoApiService;

    @Autowired
    private AddressBaseApiService addressBaseApiService;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Autowired
    private OldHouseInfoApiService oldHouseInfoApiService;

    @Autowired
    private FloorLayerInfoApiService floorLayerInfoApiService;

    @Autowired
    private HouseInfoApiService smartHouseInfoApiService;

    @Autowired
    private OldHouseholdsInfoApiService oldHouseholdsInfoApiService;

    @Autowired
    private HouseholdsInfoApiService smartHouseholdsInfoApiService;

    @Autowired
    private HouseholdIndexApiService householdIndexApiService;

    @Autowired
    private UserAccountApiService customUserAccountApiService;

    @Autowired
    private static final String DELETE = "删除";

    private static final Integer PAGE_SIZE = 30;

    @Autowired
    private CommunityApiService communityApiService;

    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private HouseInfoIndexApiService smartHouseInfoIndexApiService;

    @Autowired
    private HouseholdCertificationApiService householdCertificationApiService;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof SimpleEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<ImportEventBean> simpleEventMessage = (SimpleEventMessage<ImportEventBean>) eventMessage;
        ImportEventBean importEventBean = simpleEventMessage.getData();
        ImportTask importTask = importTaskApiService.get(importEventBean.getTaskId());
        if (importTask == null) {
            logger.error("导入任务未找到, id = " + importEventBean.getTaskId());
            return;
        }

        importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FLUSHING, "");

        switch (importEventBean.getPart()) {
            case ImportDataPartConstants.ALL:
            case ImportDataPartConstants.FLOOR_UNIT:
                this.importUnit(importTask, importEventBean);
                break;
            case ImportDataPartConstants.HOUSE:
                this.importHouse(importTask, importEventBean);
                break;
            case ImportDataPartConstants.HOUSEHOLD:
                this.importHousehold(importTask, importEventBean);
                break;
        }

    }

    private void importUnit(ImportTask importTask, ImportEventBean importEventBean) {
        AreaInfo areaInfo = smartAreaInfoApiService.findOne(importTask.getAreaId());
        if (areaInfo == null) {
            logger.error("小区信息未找到, id = " + importTask.getAreaId());
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FAILED, "刷新楼宇数据错误");
            return;
        }
        boolean isSuccess = true;
        int managerNum = 0;
        int floorNum = 0;
        int unitNum = 0;
        try {
            List<OldManagerArea> oldManagerAreaList = oldManagerAreaApiService.findAll(ApiRequest.newInstance().filterEqual(QOldManagerArea.areaId, importTask.getOldAreaId()));
            Map<Integer, Integer> managerIdMapperMap = Maps.newHashMap();
            Map<Integer, ManagerArea> managerAreaMapperMap = Maps.newHashMap();
            Map<Integer, Long> managerAddressMap = Maps.newHashMap();
            Map<Integer, Integer> floorIdMapperMap = Maps.newHashMap();
            Map<Integer, FloorInfo> floorInfoMapperMap = Maps.newHashMap();
            Map<Integer, Long> floorAddressMap = Maps.newHashMap();
            Map<Integer, Integer> floorUnitIdMapperMap = Maps.newHashMap();
            if (!CollectionUtils.isEmpty(oldManagerAreaList)) {
                List<ImportIdMapper> managerIdMappers = importIdMapperApiService.findAll(ApiRequest.newInstance().filterEqual(QImportIdMapper.extendType, ExtendType.PROJECT).filterIn(QImportIdMapper.oldId, oldManagerAreaList.stream().map(OldManagerArea::getId).collect(Collectors.toList())));
                if (!CollectionUtils.isEmpty(managerIdMappers)) {
                    managerIdMappers.forEach(managerIdMapper -> managerIdMapperMap.put(managerIdMapper.getOldId(), managerIdMapper.getNewId()));
                }
                for (OldManagerArea oldManagerArea : oldManagerAreaList) {
                    if (oldManagerArea.getStatus().equals(cn.lehome.bean.pro.old.enums.EnabledStatus.Disabled)) {
                        continue;
                    }
                    if (oldManagerArea.getAreaName().contains(DELETE)) {
                        continue;
                    }
                    Integer newId = managerIdMapperMap.get(oldManagerArea.getId());
                    ManagerArea managerArea = new ManagerArea();
                    managerArea.setAreaId(importTask.getAreaId());
                    managerArea.setAreaName(oldManagerArea.getAreaName());
                    managerArea.setManagerName(oldManagerArea.getAreaName());
                    managerArea.setStatus(cn.lehome.bean.pro.enums.EnabledStatus.Enabled);
                    if (newId != null) {
                        managerArea.setId(newId);
                        managerArea = managerAreaApiService.update(managerArea);
                    } else {
                        managerArea = managerAreaApiService.save(managerArea);
                    }

                    AddressBaseInfo addressBaseInfo = new AddressBaseInfo();
                    if (newId != null) {
                        addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.PROJECT, newId.longValue());
                        if (addressBaseInfo == null) {
                            addressBaseInfo = new AddressBaseInfo();
                        }
                    }
                    AddressBean addressBean = new AddressBean();
                    addressBean.setProjectId(managerArea.getId());
                    addressBean.setProjectName(managerArea.getAreaName());
                    addressBaseInfo.setAddress(JSON.toJSONString(addressBean));
                    addressBaseInfo.setManageAreaId(areaInfo.getId());
                    addressBaseInfo.setExtendId(managerArea.getId().longValue());
                    addressBaseInfo.setDeleteStatus(DeleteStatus.Normal);
                    addressBaseInfo.setExtendType(ExtendType.PROJECT);
                    addressBaseInfo.setParentId(0L);
                    addressBaseInfo.setName(managerArea.getAreaName());
                    if (addressBaseInfo.getId() != null) {
                        addressBaseInfo = addressBaseApiService.update(addressBaseInfo);
                    } else {
                        addressBaseInfo = addressBaseApiService.save(addressBaseInfo);
                    }
                    if (newId == null) {
                        ImportIdMapper importIdMapper = new ImportIdMapper();
                        importIdMapper.setExtendType(ExtendType.PROJECT);
                        importIdMapper.setOldId(oldManagerArea.getId());
                        importIdMapper.setNewId(managerArea.getId());
                        importIdMapperApiService.create(importIdMapper);
                    }
                    managerIdMapperMap.put(oldManagerArea.getId(), managerArea.getId());
                    managerAreaMapperMap.put(managerArea.getId(), managerArea);
                    managerAddressMap.put(managerArea.getId(), addressBaseInfo.getId());
                    managerNum++;
                }
            }

            if (!CollectionUtils.isEmpty(managerIdMapperMap)) {
                List<OldFloorInfo> oldFloorInfos = oldFloorInfoApiService.findAll(ApiRequest.newInstance().filterIn(QOldFloorInfo.manageAreaId, managerIdMapperMap.keySet()));
                if (!CollectionUtils.isEmpty(oldFloorInfos)) {
                    List<ImportIdMapper> floorIdMappers = importIdMapperApiService.findAll(ApiRequest.newInstance().filterEqual(QImportIdMapper.extendType, ExtendType.BUILDING).filterIn(QImportIdMapper.oldId, oldFloorInfos.stream().map(OldFloorInfo::getId).collect(Collectors.toList())));
                    if (!CollectionUtils.isEmpty(floorIdMappers)) {
                        floorIdMappers.forEach(floorIdMapper -> floorIdMapperMap.put(floorIdMapper.getOldId(), floorIdMapper.getNewId()));
                    }
                    for (OldFloorInfo oldFloorInfo : oldFloorInfos) {
                        if (oldFloorInfo.getEnabledStatus().equals(cn.lehome.bean.pro.old.enums.EnabledStatus.Disabled)) {
                            continue;
                        }
                        if (oldFloorInfo.getFloorNo().contains(DELETE)) {
                            continue;
                        }
                        Integer newId = floorIdMapperMap.get(oldFloorInfo.getId());
                        Integer newProjectId = managerIdMapperMap.get(oldFloorInfo.getManageAreaId());

                        if (newProjectId == null) {
                            logger.error("新管控区域ID未找到, oldManagerAreaId = " + oldFloorInfo.getManageAreaId());
                            continue;
                        }
                        ManagerArea managerArea = managerAreaMapperMap.get(newProjectId);
                        if (managerArea == null) {
                            logger.error("新管控区域ID未找到, newProjectId = " + newProjectId);
                            continue;
                        }
                        Long managerAddressId = managerAddressMap.get(newProjectId);
                        if (managerAddressId == null) {
                            logger.error("新管控区域地址ID未找到, newProjectId = " + newProjectId);
                            continue;
                        }
                        FloorInfo floorInfo = new FloorInfo();
                        floorInfo.setAreaId(importTask.getAreaId());
                        floorInfo.setFloorNo(oldFloorInfo.getFloorNo());
                        floorInfo.setManageAreaId(newProjectId);
                        floorInfo.setFloorName("");
                        floorInfo.setEnabledStatus(cn.lehome.bean.pro.enums.EnabledStatus.Enabled);
                        if (newId != null) {
                            floorInfo.setId(newId);
                            smartFloorInfoApiService.update(floorInfo);
                        } else {
                            floorInfo = smartFloorInfoApiService.save(floorInfo);
                        }

                        AddressBaseInfo addressBaseInfo = new AddressBaseInfo();
                        if (newId != null) {
                            addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.BUILDING, newId.longValue());
                            if (addressBaseInfo == null) {
                                addressBaseInfo = new AddressBaseInfo();
                            }
                        }
                        AddressBean addressBean = new AddressBean();
                        addressBean.setProjectId(newProjectId);
                        addressBean.setProjectName(managerArea.getManagerName());
                        addressBean.setBuildingId(floorInfo.getId());
                        addressBean.setBuildingName(floorInfo.getFloorName());
                        addressBean.setBuildingNumber(floorInfo.getFloorNo());
                        addressBaseInfo.setAddress(JSON.toJSONString(addressBean));
                        addressBaseInfo.setManageAreaId(areaInfo.getId());
                        addressBaseInfo.setExtendId(floorInfo.getId().longValue());
                        addressBaseInfo.setDeleteStatus(DeleteStatus.Normal);
                        addressBaseInfo.setExtendType(ExtendType.BUILDING);
                        addressBaseInfo.setParentId(managerAddressId);
                        addressBaseInfo.setName(floorInfo.getFloorNo());
                        if (addressBaseInfo.getId() != null) {
                            addressBaseInfo = addressBaseApiService.update(addressBaseInfo);
                        } else {
                            addressBaseInfo = addressBaseApiService.save(addressBaseInfo);
                        }

                        if (newId == null) {
                            ImportIdMapper importIdMapper = new ImportIdMapper();
                            importIdMapper.setExtendType(ExtendType.BUILDING);
                            importIdMapper.setOldId(oldFloorInfo.getId());
                            importIdMapper.setNewId(floorInfo.getId());
                            importIdMapperApiService.create(importIdMapper);
                        }
                        floorIdMapperMap.put(oldFloorInfo.getId(), floorInfo.getId());
                        floorInfoMapperMap.put(oldFloorInfo.getId(), floorInfo);
                        floorAddressMap.put(floorInfo.getId(), addressBaseInfo.getId());
                        floorNum++;
                    }
                }
            }

            if (!CollectionUtils.isEmpty(floorIdMapperMap)) {
                List<OldFloorUnitInfo> oldFloorUnitInfos = oldFloorUnitInfoApiService.findAll(ApiRequest.newInstance().filterIn(QFloorUnitInfo.floorId, floorIdMapperMap.keySet()));
                if (!CollectionUtils.isEmpty(oldFloorUnitInfos)) {
                    List<ImportIdMapper> floorUnitIdMappers = importIdMapperApiService.findAll(ApiRequest.newInstance().filterEqual(QImportIdMapper.extendType, ExtendType.UNIT).filterIn(QImportIdMapper.oldId, oldFloorUnitInfos.stream().map(OldFloorUnitInfo::getId).collect(Collectors.toList())));
                    if (!CollectionUtils.isEmpty(floorUnitIdMappers)) {
                        floorUnitIdMappers.forEach(floorIdMapper -> floorUnitIdMapperMap.put(floorIdMapper.getOldId(), floorIdMapper.getNewId()));
                    }

                    for (OldFloorUnitInfo oldFloorUnitInfo : oldFloorUnitInfos) {
                        if (oldFloorUnitInfo.getEnabledStatus().equals(cn.lehome.bean.pro.old.enums.EnabledStatus.Disabled)) {
                            continue;
                        }
                        if (oldFloorUnitInfo.getUnitNo().contains(DELETE)) {
                            continue;
                        }
                        Integer newId = floorUnitIdMapperMap.get(oldFloorUnitInfo.getId());
                        Integer newFloorId = floorIdMapperMap.get(oldFloorUnitInfo.getFloorId());
                        FloorInfo floorInfo = floorInfoMapperMap.get(oldFloorUnitInfo.getFloorId());
                        if (newFloorId == null || floorInfo == null) {
                            logger.error("新管楼宇ID未找到, oldFloorId = " + oldFloorUnitInfo.getFloorId());
                            continue;
                        }
                        Integer newProjectId = floorInfo.getManageAreaId();
                        ManagerArea managerArea = managerAreaMapperMap.get(newProjectId);
                        if (newProjectId == null || managerArea == null) {
                            logger.error("新管控区域ID未找到, oldManagerAreaId = " + floorInfo.getManageAreaId());
                            continue;
                        }
                        Long floorAddressId = floorAddressMap.get(newFloorId);
                        if (floorAddressId == null) {
                            logger.error("新楼宇地址ID未找到, newFloorId = " + newFloorId);
                            continue;
                        }

                        List<OldHouseInfo> oldHouseInfoList = oldHouseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QOldHouseInfo.unitId, oldFloorUnitInfo.getId()));
                        Integer maxLayerId = 0;
                        for (OldHouseInfo oldHouseInfo : oldHouseInfoList) {
                            if (oldHouseInfo.getLayerId() != null && oldHouseInfo.getLayerId() > maxLayerId) {
                                maxLayerId = oldHouseInfo.getLayerId();
                            }
                        }

                        FloorUnitInfo floorUnitInfo = new FloorUnitInfo();
                        floorUnitInfo.setUnitNo(oldFloorUnitInfo.getUnitNo());
                        floorUnitInfo.setFloorId(newFloorId);
                        floorUnitInfo.setUnitName("");
                        floorUnitInfo.setEnabledStatus(cn.lehome.bean.pro.enums.EnabledStatus.Enabled);
                        floorUnitInfo.setDescription("暂无描述");
                        List<FloorLayerInfo> floorLayerList = Lists.newArrayList();
                        if (maxLayerId > 0) {
                            floorUnitInfo.setOvergroundLayers(maxLayerId);
                            floorUnitInfo.setUndergroundLayers(0);
                            floorUnitInfo.setLayers(maxLayerId);
                            floorLayerList = getLayerList(maxLayerId);
                        } else {
                            floorUnitInfo.setOvergroundLayers(1);
                            floorUnitInfo.setUndergroundLayers(0);
                            floorUnitInfo.setLayers(1);
                            floorLayerList = getLayerList(1);
                        }

                        if (newId != null) {
                            floorUnitInfo.setId(newId);
                            floorUnitInfo = floorUnitInfoApiService.updateWithLayer(floorUnitInfo, floorLayerList);
                        } else {
                            floorUnitInfo = floorUnitInfoApiService.saveWithLayer(floorUnitInfo, floorLayerList);
                        }

                        AddressBaseInfo addressBaseInfo = new AddressBaseInfo();
                        if (newId != null) {
                            addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.UNIT, newId.longValue());
                            if (addressBaseInfo == null) {
                                addressBaseInfo = new AddressBaseInfo();
                            }
                        }
                        AddressBean addressBean = new AddressBean();
                        addressBean.setProjectId(areaInfo.getId().intValue());
                        addressBean.setProjectName(areaInfo.getAreaName());
                        addressBean.setProjectId(managerArea.getId());
                        addressBean.setProjectName(managerArea.getAreaName());
                        addressBean.setBuildingId(floorInfo.getId());
                        addressBean.setBuildingName(floorInfo.getFloorName());
                        addressBean.setBuildingNumber(floorInfo.getFloorNo());
                        addressBean.setUnitId(floorUnitInfo.getId());
                        addressBean.setUnitName(floorUnitInfo.getUnitName());
                        addressBean.setUnitNumber(floorUnitInfo.getUnitNo());
                        addressBaseInfo.setAddress(JSON.toJSONString(addressBean));
                        addressBaseInfo.setManageAreaId(areaInfo.getId());
                        addressBaseInfo.setExtendId(floorUnitInfo.getId().longValue());
                        addressBaseInfo.setDeleteStatus(DeleteStatus.Normal);
                        addressBaseInfo.setExtendType(ExtendType.UNIT);
                        addressBaseInfo.setParentId(floorAddressId);
                        addressBaseInfo.setName(floorUnitInfo.getUnitNo());
                        if (addressBaseInfo.getId() != null) {
                            addressBaseApiService.update(addressBaseInfo);
                        } else {
                            addressBaseApiService.save(addressBaseInfo);
                        }

                        if (newId == null) {
                            ImportIdMapper importIdMapper = new ImportIdMapper();
                            importIdMapper.setExtendType(ExtendType.UNIT);
                            importIdMapper.setOldId(oldFloorUnitInfo.getId());
                            importIdMapper.setNewId(floorUnitInfo.getId());
                            importIdMapperApiService.create(importIdMapper);
                        }
                        unitNum++;
                    }
                }
            }


        } catch (Exception e) {
            logger.error("导入楼宇信息错误 : ", e);
            isSuccess = false;
        }

        importTask.setProjectNum(managerNum);
        importTask.setFloorNum(floorNum);
        importTask.setUnitNum(unitNum);
        importTaskApiService.updateNum(importTask);
        if (!isSuccess) {
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FAILED, "导入楼宇错误");
            return;
        }

        importEventBean.setPart(ImportDataPartConstants.HOUSE);
        importEventBean.setStartId(0);
        eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.IMPORT_AREA_DATA_EVENT, importEventBean));
    }

    private void importHouse(ImportTask importTask, ImportEventBean importEventBean) {
        ApiResponse<OldHouseInfo> response = oldHouseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QOldHouseInfo.areaId, importTask.getOldAreaId()).filterGreaterThan(QOldHouseInfo.id, importEventBean.getStartId()), ApiRequestPage.newInstance().paging(0, PAGE_SIZE).addOrder(QOldHouseInfo.id, PageOrderType.ASC));
        int houseNum = 0;
        int delHouseNum = 0;
        int lastId = 0;
        boolean isSuccess = true;
        boolean isFirst = false;
        if (importEventBean.getStartId() == 0) {
            isFirst = true;
        }
        if (!CollectionUtils.isEmpty(response.getPagedData())) {
            Set<Integer> oldUnitIdSet = response.getPagedData().stream().map(OldHouseInfo::getUnitId).collect(Collectors.toSet());
            List<ImportIdMapper> unitIdMapperList = importIdMapperApiService.findAll(ApiRequest.newInstance().filterEqual(QImportIdMapper.extendType, ExtendType.UNIT).filterIn(QImportIdMapper.oldId, oldUnitIdSet));
            Map<Integer, Integer> unitIdMap = Maps.newHashMap();
            Map<Integer, AddressBaseInfo> unitAddressMap = Maps.newHashMap();
            if (!CollectionUtils.isEmpty(unitIdMapperList)) {
                unitIdMapperList.forEach(mapper -> unitIdMap.put(mapper.getOldId(), mapper.getNewId()));
                List<AddressBaseInfo> addressBaseInfos = addressBaseApiService.findAll(ApiRequest.newInstance().filterEqual(QAddressBaseInfo.extendType, ExtendType.UNIT).filterIn(QAddressBaseInfo.extendId, unitIdMap.values()));
                if (!CollectionUtils.isEmpty(addressBaseInfos)) {
                    addressBaseInfos.forEach(address -> unitAddressMap.put(address.getExtendId().intValue(), address));
                }
            }
            for (OldHouseInfo oldHouseInfo : response.getPagedData()) {
                try {
                    if (oldHouseInfo.getEnabledStatus().equals(cn.lehome.bean.pro.enums.EnabledStatus.Disabled)) {
                        delHouseNum += 1;
                    } else {
                        Integer newUnitId = unitIdMap.get(oldHouseInfo.getUnitId());
                        if (newUnitId == null) {
                            logger.error("新的单元ID未找到, unitId = " + oldHouseInfo.getUnitId());
                            delHouseNum++;
                            lastId = oldHouseInfo.getId();
                            continue;
                        }
                        AddressBaseInfo unitAddressBaseInfo = unitAddressMap.get(newUnitId);
                        if (unitAddressBaseInfo == null) {
                            logger.error("新的单元地址信息未找到, unitId = " + oldHouseInfo.getUnitId());
                            delHouseNum++;
                            lastId = oldHouseInfo.getId();
                            continue;
                        }
                        int layerId = 1;
                        if (oldHouseInfo.getLayerId() > 0) {
                            lastId = oldHouseInfo.getLayerId();
                        }
                        List<FloorLayerInfo> floorLayerInfos = floorLayerInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QFloorLayerInfo.unitId, newUnitId).filterEqual(QFloorLayerInfo.type, FloorsType.aboveground).filterEqual(QFloorLayerInfo.number, layerId));
                        if (CollectionUtils.isEmpty(floorLayerInfos)) {
                            logger.error("楼层信息未找到, unitId = {}, layerId = {}", newUnitId, layerId);
                            delHouseNum++;
                            lastId = oldHouseInfo.getId();
                            continue;
                        }
                        FloorLayerInfo floorLayerInfo = floorLayerInfos.get(0);
                        AddressBean addressBean = JSON.parseObject(unitAddressBaseInfo.getAddress(), AddressBean.class);
                        HouseInfo houseInfo = BeanMapping.map(oldHouseInfo, HouseInfo.class);
                        houseInfo.setAreaId(importTask.getAreaId());
                        houseInfo.setManageAreaId(addressBean.getProjectId().intValue());
                        houseInfo.setFloorId(addressBean.getBuildingId().intValue());
                        houseInfo.setUnitId(addressBean.getUnitId().intValue());
                        houseInfo.setLayerId(floorLayerInfo.getId());
                        houseInfo.setDecorationStatus(DecorationStatus.ROUGH);
                        houseInfo.setRoomType(RoomType.HOUSE);
                        houseInfo.setOccupancyTime(oldHouseInfo.getOccupancyTime());
                        houseInfo.setStartChargingTime(oldHouseInfo.getStartChargingTime());
                        String roomAddress = houseInfo.getManagerAreaName();
                        if (StringUtils.isNotEmpty(houseInfo.getFloorNo())) {
                            roomAddress += "-" + houseInfo.getFloorNo();
                            if (StringUtils.isNotEmpty(houseInfo.getFloorName())) {
                                roomAddress += houseInfo.getFloorName();
                            }
                        }
                        if (StringUtils.isNotEmpty(houseInfo.getUnitNo())) {
                            roomAddress += "-" + houseInfo.getUnitNo();
                            if (StringUtils.isNotEmpty(houseInfo.getUnitName())) {
                                roomAddress += houseInfo.getUnitName();
                            }
                        }
                        roomAddress += "-" + houseInfo.getRoomId();
                        if (StringUtils.isNotEmpty(houseInfo.getRoomName())) {
                            roomAddress += houseInfo.getRoomName();
                        }
                        houseInfo.setRoomAddress(roomAddress);
                        houseInfo.setOccupancyStatus(OccupancyStatus.OCCUPANCY);
                        houseInfo.setRentStatus(RentStatus.NOT_RENT);
                        houseInfo.setSaleStatus(SaleStatus.SOLD);

                        Integer newId = null;
                        List<ImportIdMapper> importIdMappers = importIdMapperApiService.findAll(ApiRequest.newInstance().filterEqual(QImportIdMapper.extendType, ExtendType.HOUSE).filterEqual(QImportIdMapper.oldId, oldHouseInfo.getId()));
                        if (!CollectionUtils.isEmpty(importIdMappers)) {
                            newId = importIdMappers.get(0).getNewId();
                        }

                        if (newId != null) {
                            houseInfo.setId(newId);
                            houseInfo = smartHouseInfoApiService.update(houseInfo);
                        } else {
                            houseInfo = smartHouseInfoApiService.save(houseInfo);
                        }

                        addressBean.setHouseName(houseInfo.getRoomName());
                        addressBean.setHouseNumber(houseInfo.getRoomId());
                        addressBean.setHouseId(houseInfo.getId().toString());
                        AddressBaseInfo addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.HOUSE, houseInfo.getId().longValue());
                        if (addressBaseInfo == null) {
                            addressBaseInfo = new AddressBaseInfo();
                        }
                        addressBaseInfo.setAddress(JSON.toJSONString(addressBean));
                        addressBaseInfo.setManageAreaId(importTask.getAreaId().longValue());
                        addressBaseInfo.setExtendId(houseInfo.getId().longValue());
                        addressBaseInfo.setDeleteStatus(DeleteStatus.Normal);
                        addressBaseInfo.setExtendType(ExtendType.HOUSE);
                        addressBaseInfo.setParentId(unitAddressBaseInfo.getId());
                        addressBaseInfo.setName(houseInfo.getRoomId());
                        if (addressBaseInfo.getId() != null) {
                            addressBaseApiService.update(addressBaseInfo);
                        } else {
                            addressBaseApiService.save(addressBaseInfo);
                        }

                        if (newId == null) {
                            ImportIdMapper importIdMapper = new ImportIdMapper();
                            importIdMapper.setExtendType(ExtendType.HOUSE);
                            importIdMapper.setOldId(oldHouseInfo.getId());
                            importIdMapper.setNewId(houseInfo.getId());
                            importIdMapperApiService.create(importIdMapper);
                        }

                        houseNum++;
                    }
                    lastId = oldHouseInfo.getId();
                } catch (Exception e) {
                    logger.error("刷新数据出错, id = " + oldHouseInfo.getId(), e);
                    isSuccess = false;
                    break;
                }
            }
        }

        if (!isFirst) {
            importTask.setHouseNum(importTask.getHouseNum() + houseNum);
            importTask.setDelHouseNum(importTask.getDelHouseNum() + delHouseNum);
        } else {
            importTask.setHouseNum(houseNum);
            importTask.setDelHouseNum(delHouseNum);
        }

        importTaskApiService.updateNum(importTask);
        if (!isSuccess) {
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FAILED, "刷新房间错误");
            return;
        }


        if (response.getCount() == 0 || response.getCount() < PAGE_SIZE) {
            if (!importEventBean.isContinue()) {
                importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FINISHED, "");
            } else {
                importEventBean.setPart(ImportDataPartConstants.HOUSEHOLD);
                importEventBean.setStartId(0);
                eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.IMPORT_AREA_DATA_EVENT, importEventBean));
            }
        } else {
            importEventBean.setPart(ImportDataPartConstants.HOUSE);
            importEventBean.setStartId(lastId);
            eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.IMPORT_AREA_DATA_EVENT, importEventBean));
        }
    }


    private void importHousehold(ImportTask importTask, ImportEventBean importEventBean) {
        AreaInfo areaInfo = smartAreaInfoApiService.findOne(importTask.getAreaId());
        boolean isFirst = false;
        if (areaInfo == null) {
            logger.error("小区信息未找到, id = " + importTask.getAreaId());
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FAILED, "刷新住户数据错误");
            return;
        }
        if (importEventBean.getStartId() == 0) {
            isFirst = true;
        }
        ApiResponse<OldHouseholdsSettingsInfo> response = oldHouseholdsInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseholdsSettingsInfo.areaId, importTask.getOldAreaId()).filterGreaterThan(QHouseholdsSettingsInfo.id, importEventBean.getStartId()), ApiRequestPage.newInstance().paging(0, PAGE_SIZE).addOrder(QHouseholdsSettingsInfo.id, PageOrderType.ASC));
        int householdNum = 0;
        int delHouseholdNum = 0;
        int repeatNum = 0;
        int lastId = 0;
        boolean isSuccess = true;
        if (!CollectionUtils.isEmpty(response.getPagedData())) {
            Set<Integer> oldHouseIdSet = response.getPagedData().stream().map(OldHouseholdsSettingsInfo::getHouseId).collect(Collectors.toSet());
            List<ImportIdMapper> houseIdMapperList = importIdMapperApiService.findAll(ApiRequest.newInstance().filterEqual(QImportIdMapper.extendType, ExtendType.HOUSE).filterIn(QImportIdMapper.oldId, oldHouseIdSet));
            Map<Integer, Integer> houseIdMap = Maps.newHashMap();
            Map<Integer, AddressBaseInfo> houseAddressMap = Maps.newHashMap();
            if (!CollectionUtils.isEmpty(houseIdMapperList)) {
                houseIdMapperList.forEach(mapper -> houseIdMap.put(mapper.getOldId(), mapper.getNewId()));
                List<AddressBaseInfo> addressBaseInfos = addressBaseApiService.findAll(ApiRequest.newInstance().filterEqual(QAddressBaseInfo.extendType, ExtendType.HOUSE).filterIn(QAddressBaseInfo.extendId, houseIdMap.values()));
                if (!CollectionUtils.isEmpty(addressBaseInfos)) {
                    addressBaseInfos.forEach(address -> houseAddressMap.put(address.getExtendId().intValue(), address));
                }
            }
            Map<Integer, OldHouseholdsInfo> oldHouseholdsInfoMap = oldHouseholdsInfoApiService.findAll(response.getPagedData().stream().map(OldHouseholdsSettingsInfo::getHouseholdsId).collect(Collectors.toList()));

            for (OldHouseholdsSettingsInfo oldHouseholdsSettingsInfo : response.getPagedData()) {
                try {
                    if (oldHouseholdsSettingsInfo.getDeleteStatus().equals(RecordDeleteStatus.Deleted)) {
                        delHouseholdNum++;
                        lastId = oldHouseholdsSettingsInfo.getId();
                        continue;
                    }
                    OldHouseholdsInfo oldHouseholdsInfo = oldHouseholdsInfoMap.get(oldHouseholdsSettingsInfo.getHouseholdsId());
                    if (oldHouseholdsInfo == null) {
                        logger.error("住户信息未找到, householdId = " + oldHouseholdsSettingsInfo.getHouseholdsId());
                        delHouseholdNum++;
                        lastId = oldHouseholdsSettingsInfo.getId();
                        continue;
                    }
                    Integer newHouseId = houseIdMap.get(oldHouseholdsSettingsInfo.getHouseId());
                    if (newHouseId == null) {
                        logger.error("房产信息未找到, houseId = " + oldHouseholdsSettingsInfo.getHouseId());
                        delHouseholdNum++;
                        lastId = oldHouseholdsSettingsInfo.getId();
                        continue;
                    }
                    AddressBaseInfo houseAddress = houseAddressMap.get(newHouseId);
                    if (houseAddress == null) {
                        logger.error("房产信息未找到, newHouseId = " + newHouseId);
                        delHouseholdNum++;
                        lastId = oldHouseholdsSettingsInfo.getId();
                        continue;
                    }
                    Integer newId = null;
                    List<ImportIdMapper> importIdMappers = importIdMapperApiService.findAll(ApiRequest.newInstance().filterEqual(QImportIdMapper.extendType, ExtendType.EQUIPMENT_ROOM).filterEqual(QImportIdMapper.oldId, oldHouseholdsSettingsInfo.getId()));
                    if (!CollectionUtils.isEmpty(importIdMappers)) {
                        newId = importIdMappers.get(0).getNewId();
                    }
                    List<HouseholdIndex> householdIndexList = householdIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseholdIndex.houseId, oldHouseholdsSettingsInfo.getHouseId()).filterEqual(QHouseholdIndex.telephone, oldHouseholdsInfo.getTelephone()));
                    if (!CollectionUtils.isEmpty(householdIndexList)) {
                        boolean isFailed = true;
                        if (newId != null) {
                            HouseholdIndex householdIndex = householdIndexList.get(0);
                            if (householdIndex.getId().equals(newId.longValue())) {
                                isFailed = false;
                            }
                        }
                        if (isFailed) {
                            logger.error("住户信息已经存在，houseID= {}, telephone = {}", oldHouseholdsSettingsInfo.getHouseId(), oldHouseholdsInfo.getTelephone());
                            repeatNum++;
                            lastId = oldHouseholdsSettingsInfo.getId();
                            continue;
                        }
                    }
                    AddressBean addressBean = JSON.parseObject(houseAddress.getAddress(), AddressBean.class);
                    Household household = this.patch(oldHouseholdsInfo, oldHouseholdsSettingsInfo, addressBean, newId, houseAddress.getId(), areaInfo);
                    if (household.getId() != null) {
                        smartHouseholdsInfoApiService.update(household);
                    } else {
                        household = this.createHousehold(household);
                    }
                    if (newId == null) {
                        ImportIdMapper importIdMapper = new ImportIdMapper();
                        importIdMapper.setExtendType(ExtendType.EQUIPMENT_ROOM);
                        importIdMapper.setOldId(oldHouseholdsSettingsInfo.getId());
                        importIdMapper.setNewId(household.getId().intValue());
                        importIdMapperApiService.create(importIdMapper);
                    }
                    householdNum++;
                    lastId = oldHouseholdsSettingsInfo.getId();
                } catch (Exception e) {
                    logger.error("导入住户数据失败 ： " + oldHouseholdsSettingsInfo.getId(), e);
                    isSuccess = false;
                    break;
                }
            }
        }
        if (!isFirst) {
            importTask.setHouseholdNum(importTask.getHouseholdNum() + householdNum);
            importTask.setDelHouseholdNum(importTask.getDelHouseholdNum() + delHouseholdNum);
            importTask.setRepeatHouseholdNum(importTask.getRepeatHouseholdNum() + repeatNum);
        } else {
            importTask.setHouseholdNum(householdNum);
            importTask.setDelHouseholdNum(delHouseholdNum);
            importTask.setRepeatHouseholdNum(repeatNum);
        }

        importTaskApiService.updateNum(importTask);
        if (!isSuccess) {
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FAILED, "导入住户错误");
            return;
        }


        if (response.getCount() == 0 || response.getCount() < PAGE_SIZE) {
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FINISHED, "");
        } else {
            importEventBean.setPart(ImportDataPartConstants.HOUSEHOLD);
            importEventBean.setStartId(lastId);
            eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.IMPORT_AREA_DATA_EVENT, importEventBean));
        }
    }


    @Override
    public String getConsumerId() {
        return "area_import";
    }

    private List<FloorLayerInfo> getLayerList(Integer maxLayId) {
        List<FloorLayerInfo> floorLayerInfos = Lists.newArrayList();
        for (int i = 1; i <= maxLayId; i++) {
            FloorLayerInfo floorLayerInfo = new FloorLayerInfo();
            floorLayerInfo.setType(FloorsType.aboveground);
            floorLayerInfo.setNumber(String.valueOf(i));
            floorLayerInfo.setName("层");
            floorLayerInfos.add(floorLayerInfo);
        }
        return floorLayerInfos;
    }

    public Household createHousehold(Household household) {
        boolean isNew = false;
        if (StringUtils.isNotEmpty(household.getTelephone())) {
            UserAccount userAccount = customUserAccountApiService.getByPhone(household.getTelephone());
            if (userAccount == null) {
                SexType sexType;
                if (household.getGender() == Gender.Male) {
                    sexType = SexType.Male;
                } else if (household.getGender() == Gender.Female) {
                    sexType = SexType.Female;
                } else {
                    sexType = SexType.Unknown;
                }
                userAccount = customUserAccountApiService.createBySmartPro(household.getTelephone(), household.getName(), sexType);
                isNew = true;
            }
            household.setUserId(userAccount.getId().intValue());
        }
        household.setApplyChannel(ApplyChannel.ApplyByStaff);
        household = smartHouseholdsInfoApiService.save(household);
        logger.info("发送添加住户, id = " + household.getId());
        eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.HOUSEHOLD_ENTRANCE_AUTH_EVENT, household.getId()));
        if (!isNew) {
            CommunityExt communityExt = communityApiService.findByPropertyAreaId(household.getAreaId());
            if (communityExt != null && StringUtils.isNotEmpty(household.getTelephone())) {
                UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.findByRemark(household.getTelephone(), communityExt.getId(), household.getHouseId().longValue());
                if (userHouseRelationship != null) {
                    userHouseRelationship.setHouseType(convert(household.getHouseholdsTypeId().intValue()));
                    userHouseRelationship.setEnableStatus(EnableDisableStatus.ENABLE);
                    userHouseRelationshipApiService.update(userHouseRelationship);
                } else {
                    UserInfoIndex userInfoIndex = userInfoIndexApiService.findByPhone(household.getTelephone());
                    HouseInfoIndex houseInfoIndex = smartHouseInfoIndexApiService.get(household.getHouseId().longValue());
                    if (userInfoIndex != null && houseInfoIndex != null) {
                        userHouseRelationship = new UserHouseRelationship();
                        userHouseRelationship.setEnableStatus(EnableDisableStatus.ENABLE);
                        userHouseRelationship.setOperatorId(0L);
                        userHouseRelationship.setHouseType(convert(household.getHouseholdsTypeId().intValue()));
                        userHouseRelationship.setRemark(household.getTelephone());
                        userHouseRelationship.setFamilyMemberName(household.getName());
                        userHouseRelationship.setCommunityExtId(communityExt.getId());
                        userHouseRelationship.setHouseId(household.getHouseId().longValue());
                        userHouseRelationship.setUserId(userInfoIndex.getId());
                        userHouseRelationship.setHouseAddress(houseInfoIndex.getRoomAddress());
                        userHouseRelationship.setFullHouseAddress(String.format("%s%s", communityExt.getName(), houseInfoIndex.getRoomAddress()));
                        userHouseRelationshipApiService.saveUserHouse(userHouseRelationship);
                    }
                }
            }
        }
        this.syncHouseholdCertification(household);
        return household;
    }

    private Household patch(OldHouseholdsInfo oldHouseholdsInfo, OldHouseholdsSettingsInfo oldHouseholdsSettingsInfo, AddressBean houseBean, Integer newId, Long addressid, AreaInfo areaInfo) {
        Household household = new Household();
        if (newId != null) {
            household.setId(Long.valueOf(newId));
        }
        household.setName(oldHouseholdsInfo.getName());
        household.setCompany("");
        household.setWorkplaceTel("");
        household.setWorkplaceAddress("");
        household.setTelephone(oldHouseholdsInfo.getTelephone());
        household.setSpareTelephone(oldHouseholdsInfo.getSpareTelephone());
        household.setHousePhone("");
        household.setMailingAddress(oldHouseholdsInfo.getEmail());
        household.setSpareAddress("");
        if (oldHouseholdsInfo.getBirthday() != null) {
            household.setBirthday(oldHouseholdsInfo.getBirthday());
        }
        if (oldHouseholdsInfo.getGender().equals(cn.lehome.bean.pro.old.enums.household.Gender.Male)) {
            household.setGender(Gender.Male);
        } else if (oldHouseholdsInfo.getGender().equals(cn.lehome.bean.pro.old.enums.household.Gender.Female)) {
            household.setGender(Gender.Female);
        } else {
            household.setGender(Gender.Unknown);
        }
        household.setIschild(0);
        household.setIslivein(0);
        household.setIsold(0);
        household.setChangeStatus(YesOrNo.No);

        // 12.1日更新, HouseholdsInfoBizImpl中有这个, 不清楚为什么
        household.setEnabledStatus(cn.lehome.bean.pro.enums.EnabledStatus.Enabled);
        household.setFloorId(houseBean.getBuildingId().longValue());
        household.setManageAreaId(houseBean.getProjectId().longValue());
        household.setUnitId(houseBean.getUnitId().longValue());
        household.setHouseId(Long.valueOf(houseBean.getHouseId()));
        household.setAddressId(addressid.intValue());
        household.setPropertyCertificateStatus(YesOrNo.Yes);
        household.setPropertyCertificateStatus(YesOrNo.No);
        household.setUniqueCode(areaInfo.getUniqueCode());

        if (oldHouseholdsSettingsInfo.getIsMain().equals(cn.lehome.bean.pro.old.enums.YesOrNo.Yes)) {
            household.setIsMain(YesOrNo.Yes);
        } else if (oldHouseholdsSettingsInfo.getIsMain().equals(cn.lehome.bean.pro.old.enums.YesOrNo.No)) {
            household.setIsMain(YesOrNo.No);
        } else {
            household.setIsMain(YesOrNo.Default);
        }
        household.setHouseholdsTypeId(oldHouseholdsSettingsInfo.getHouseholdsTypeId().longValue());

        return household;
    }

    private cn.lehome.bean.user.entity.enums.user.HouseType convert(Integer householdTypeId) {
        if (householdTypeId.equals(Identity.resident_owner.index())) {
            return cn.lehome.bean.user.entity.enums.user.HouseType.MAIN;
        } else if (householdTypeId.equals(Identity.resident_relative.index())) {
            return cn.lehome.bean.user.entity.enums.user.HouseType.HOME;
        } else if (householdTypeId.equals(Identity.resident_renter.index())) {
            return cn.lehome.bean.user.entity.enums.user.HouseType.RENTER;
        } else {
            return cn.lehome.bean.user.entity.enums.user.HouseType.OTHER;
        }
    }

    private void syncHouseholdCertification(Household household) {
        HouseholdCertification bean = new HouseholdCertification();
        bean.setAreaId(household.getAreaId());
        bean.setHouseId(household.getHouseId());
        bean.setUserId(household.getUserId() != null ? household.getUserId().longValue() : null);
        bean.setUserName(household.getName());
        bean.setMobile(org.springframework.util.StringUtils.isEmpty(household.getTelephone()) ? "" : household.getTelephone());
        bean.setIdentifyType(convertType(household.getHouseholdsTypeId()));
        bean.setApplyTime(new Date());
        bean.setApprovalStatus(ApprovalStatus.PASSED);
        bean.setApplyChannel(ApplyChannel.ApplyByStaff);
        bean.setCancelChannel(CancelChannel.CancelDefault);
        bean.setApprovalUserId(0L);
        bean.setApprovalTime(new Date());
        bean.setHouseholdId(household.getHouseholdsId());
        bean.setHouseholdSettingId(household.getHouseholdsSettingsId());
        bean.setClientType(CertifiedClientType.WEB);
        bean.setSourceType(CertifiedSourceType.ADD_HOUSEHOLD);
        householdCertificationApiService.save(bean);
    }

    private IdentifyType convertType(Long householdTypeId) {
        if (householdTypeId == null || householdTypeId <= 0) {
            return IdentifyType.OTHER;
        }
        switch (getType(householdTypeId.intValue())) {
            case Owner:
                return IdentifyType.OWNER;
            case Merchant:
                return IdentifyType.MERCHANT;
            case Household:
                return IdentifyType.HOUSEHOLD;
            case Tenant:
                return IdentifyType.TENANT;
            case RELATIVE:
                return IdentifyType.RELATIVE;
            default:
                return IdentifyType.OTHER;
        }
    }

    private HouseholdsType getType(Integer id) {
        return Stream.of(HouseholdsType.values()).filter(t -> t.index().equals(id)).findFirst().orElse(null);
    }



}
