package cn.lehome.dispatcher.queue.listener.dataImport;

import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.base.pro.api.bean.area.AreaInfo;
import cn.lehome.base.pro.api.bean.area.ManagerArea;
import cn.lehome.base.pro.api.bean.area.QAreaInfo;
import cn.lehome.base.pro.api.bean.area.QManagerArea;
import cn.lehome.base.pro.api.bean.data.*;
import cn.lehome.base.pro.api.bean.house.*;
import cn.lehome.base.pro.api.bean.house.layout.ApartmentLayout;
import cn.lehome.base.pro.api.bean.house.layout.QApartmentLayout;
import cn.lehome.base.pro.api.event.DataImportEvent;
import cn.lehome.base.pro.api.service.area.AreaInfoApiService;
import cn.lehome.base.pro.api.service.area.ManagerAreaApiService;
import cn.lehome.base.pro.api.service.data.DataImportApiService;
import cn.lehome.base.pro.api.service.house.*;
import cn.lehome.bean.pro.enums.DeleteStatus;
import cn.lehome.bean.pro.enums.EnabledStatus;
import cn.lehome.bean.pro.enums.Gender;
import cn.lehome.bean.pro.enums.Identity;
import cn.lehome.bean.pro.enums.data.DataImportStatus;
import cn.lehome.bean.pro.enums.data.DataImportType;
import cn.lehome.bean.pro.enums.house.DecorationStatus;
import cn.lehome.bean.pro.enums.house.FloorsType;
import cn.lehome.bean.pro.enums.house.OccupancyStatus;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.ExcelUtils;
import cn.lehome.framework.base.api.core.util.OssFileDownloadUtil;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;

/**
 * Created by wuzhao on 2019/5/31.
 */
public class ImportDataListener extends AbstractJobListener {

    @Autowired
    private DataImportApiService dataImportApiService;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Autowired
    private AreaInfoApiService smartAreaInfoApiService;

    @Autowired
    private ManagerAreaApiService managerAreaApiService;

    @Autowired
    private FloorInfoApiService smartFloorInfoApiService;

    @Autowired
    private FloorUnitInfoApiService floorUnitInfoApiService;

    @Autowired
    private FloorLayerInfoApiService floorLayerInfoApiService;

    @Autowired
    private HouseInfoApiService smartHouseInfoApiService;

    @Autowired
    private ApartmentLayoutApiService apartmentLayoutApiService;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof SimpleEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<DataImportEvent> simpleEventMessage = (SimpleEventMessage<DataImportEvent>) eventMessage;
        DataImportEvent dataImportEvent = simpleEventMessage.getData();
        DataImport dataImport = dataImportApiService.get(dataImportEvent.getDataImportId());
        if (dataImport == null) {
            logger.error("数据导入记录未找到, id = " + dataImportEvent.getDataImportId());
            if (dataImportEvent.getPre()) {
                dataImport.setStatus(DataImportStatus.PRE_IMPORT_FAILED);
            } else {
                dataImport.setStatus(DataImportStatus.CANCEL);
            }
        }


        if (dataImportEvent.getPre()) {
            boolean isSuccess = true;
            String errorMsg = "";
            try {
                OssFileDownloadUtil ossFileDownloadUtil = new OssFileDownloadUtil(dataImport.getExcelUrl());
                String filePath = ossFileDownloadUtil.downloadFileFromOss();
                ExcelUtils excelUtils = new ExcelUtils(filePath);
                List<List<String>> datas = excelUtils.read(0, dataImportEvent.getObjectId().intValue() - 1, dataImportEvent.getObjectId().intValue());
                if (dataImportEvent.getType().equals(DataImportType.HOUSE)) {
                    Pair<Boolean, String> resultPair = this.preHouse(datas, dataImport.getAreaId(), dataImport.getId());
                    if (!resultPair.getLeft()) {
                        isSuccess = false;
                        errorMsg = resultPair.getRight();
                    }
                } else {
                    Pair<Boolean, String> resultPair = this.preHousehold(datas, dataImport.getAreaId(), dataImport.getId());
                    if (!resultPair.getLeft()) {
                        isSuccess = false;
                        errorMsg = resultPair.getRight();
                    }
                }
            } catch (Exception e) {
                logger.error("预导入失败, dataImportId = {}, line = {}", dataImport.getId(), dataImportEvent.getObjectId());
                isSuccess = false;
                errorMsg = "系统错误";
            } finally {
                if (isSuccess) {
                    dataImportApiService.addPreSuccess(dataImport.getId());
                } else {
                    DataImportFailedRecord dataImportFailedRecord = new DataImportFailedRecord();
                    dataImportFailedRecord.setDataImportId(dataImport.getId());
                    dataImportFailedRecord.setErrorMsg(errorMsg);
                    dataImportFailedRecord.setIsPre(YesNoStatus.YES);
                    dataImportFailedRecord.setObjectId(dataImportEvent.getObjectId());
                    dataImportApiService.addPreFailed(dataImport.getId(), dataImportFailedRecord);
                }
                Integer nextLine = dataImportEvent.getObjectId().intValue() + 1;
                if ( nextLine > dataImport.getExcelMaxLine()) {
                    eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.DATA_IMPORT_EVENT, new DataImportEvent(dataImport.getId(), dataImport.getType(), true, nextLine.longValue())));
                } else {
                    if (dataImport.getPreFailedNum() != 0) {
                        dataImport.setStatus(DataImportStatus.PRE_IMPORT_FAILED);
                    } else {
                        dataImport.setStatus(DataImportStatus.PRE_IMPORT_FINISHED);
                    }
                    dataImportApiService.update(dataImport);
                }
            }
        } else {
            dataImportApiService.addImportNum(dataImport.getId(), dataImport.getType(), dataImportEvent.getObjectId().intValue());
            Long nextId = 0L;
            if (dataImportEvent.getType().equals(DataImportType.HOUSE)) {
                ApiResponse<DataImportHouseInfo> response = dataImportApiService.findHouseAll(ApiRequest.newInstance().filterEqual(QDataImportHouseInfo.dataImportId, dataImport).filterGreaterThan(QDataImportHouseInfo.id, dataImportEvent.getObjectId().intValue()), ApiRequestPage.newInstance().paging(0, 1).addOrder(QDataImportHouseInfo.id, PageOrderType.ASC));
                if (!CollectionUtils.isEmpty(response.getPagedData())) {
                    nextId = Lists.newArrayList(response.getPagedData()).get(0).getId().longValue();
                }
            } else {
                ApiResponse<DataImportHouseholdsInfo> response = dataImportApiService.findHouseholdAll(ApiRequest.newInstance().filterEqual(QDataImportHouseInfo.dataImportId, dataImport).filterGreaterThan(QDataImportHouseholdsInfo.id, dataImportEvent.getObjectId().intValue()), ApiRequestPage.newInstance().paging(0, 1).addOrder(QDataImportHouseInfo.id, PageOrderType.ASC));
                if (!CollectionUtils.isEmpty(response.getPagedData())) {
                    nextId = Lists.newArrayList(response.getPagedData()).get(0).getId().longValue();
                }
            }
            if (nextId != 0L) {
                eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.DATA_IMPORT_EVENT, new DataImportEvent(dataImport.getId(), dataImport.getType(), false, nextId)));
            } else {
                dataImport.setStatus(DataImportStatus.IMPORT_FINISHED);
                dataImportApiService.update(dataImport);
            }
        }

    }

    private Pair<Boolean, String> preHouse(List<List<String>> datas, Long areaId, Long dataImportId) throws Exception {
        if (datas == null || datas.size() != 1) {
            return new ImmutablePair<>(false, "未读取到数据");
        }
        List<String> rowDatas = datas.get(0);
        if (rowDatas.size() < 17) {
            return new ImmutablePair<>(false, "数据列数不符合");
        }
        String areaName = rowDatas.get(0);
        List<AreaInfo> areaInfoList = smartAreaInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QAreaInfo.areaName, areaName));
        if (areaInfoList == null || areaInfoList.size() < 1 || !areaInfoList.get(0).getId().equals(areaId)) {
            return new ImmutablePair<>(false, "未找到小区信息");
        }
        AreaInfo areaInfo = areaInfoList.get(0);
        String managerName = rowDatas.get(1);
        List<ManagerArea> managerAreaList = managerAreaApiService.findAll(ApiRequest.newInstance().filterEqual(QManagerArea.areaId, areaId).filterEqual(QManagerArea.areaName, managerName).filterEqual(QManagerArea.status, EnabledStatus.Enabled));
        if (managerAreaList == null || managerAreaList.size() < 1) {
            return new ImmutablePair<>(false, "未找到小区项目信息");
        }
        ManagerArea managerArea = managerAreaList.get(0);
        String floorNo = rowDatas.get(2);
        List<FloorInfo> floorInfoList = smartFloorInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QFloorInfo.manageAreaId, managerArea.getId()).filterEqual(QFloorInfo.floorNo, floorNo).filterEqual(QFloorInfo.enabledStatus, EnabledStatus.Enabled));
        if (floorInfoList == null || floorInfoList.size() < 1) {
            return new ImmutablePair<>(false, "未找到小区楼宇信息");
        }
        FloorInfo floorInfo = floorInfoList.get(0);
        String unitNo = rowDatas.get(4);
        List<FloorUnitInfo> floorUnitInfoList = floorUnitInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QFloorUnitInfo.floorId, floorInfo.getId()).filterEqual(QFloorUnitInfo.unitNo, unitNo).filterEqual(QFloorUnitInfo.enabledStatus, EnabledStatus.Enabled));
        if (floorUnitInfoList == null || floorUnitInfoList.size() < 1) {
            return new ImmutablePair<>(false, "未找到小区单元信息");
        }
        FloorUnitInfo unitInfo = floorUnitInfoList.get(0);
        String upLayer = rowDatas.get(6);
        String downLayer = rowDatas.get(7);
        if (StringUtils.isEmpty(upLayer) && StringUtils.isEmpty(downLayer)) {
            return new ImmutablePair<>(false, "楼层信息不能为空");
        }
        List<FloorLayerInfo> layerInfoList = null;
        if (StringUtils.isNotEmpty(upLayer)) {
            layerInfoList = floorLayerInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QFloorLayerInfo.unitId, unitInfo.getId()).filterEqual(QFloorLayerInfo.type, FloorsType.aboveground).filterEqual(QFloorLayerInfo.number, upLayer).filterEqual(QFloorLayerInfo.deleteStatus, DeleteStatus.Normal));
        }
        if (StringUtils.isNotEmpty(downLayer)) {
            layerInfoList = floorLayerInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QFloorLayerInfo.unitId, unitInfo.getId()).filterEqual(QFloorLayerInfo.type, FloorsType.underground).filterEqual(QFloorLayerInfo.number, upLayer).filterEqual(QFloorLayerInfo.deleteStatus, DeleteStatus.Normal));
        }
        if (layerInfoList == null || layerInfoList.size() < 1) {
            return new ImmutablePair<>(false, "未找到小区楼层信息");
        }
        FloorLayerInfo floorLayerInfo = layerInfoList.get(0);
        String roomId = rowDatas.get(8);
        String roomName = rowDatas.get(9);
        List<HouseInfo> houseInfoList = smartHouseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseInfo.unitId, unitInfo.getId()).filterEqual(QHouseInfo.roomId, roomId).filterEqual(QHouseInfo.enabledStatus, EnabledStatus.Enabled));
        if (!CollectionUtils.isEmpty(houseInfoList)) {
            return new ImmutablePair<>(false, "房间信息已经存在");
        }
        String roomType = rowDatas.get(10);
        ApartmentLayout apartmentLayout = null;
        if (StringUtils.isEmpty(roomType)) {
            List<ApartmentLayout> apartmentLayoutList = apartmentLayoutApiService.findAll(ApiRequest.newInstance().filterEqual(QApartmentLayout.areaId, areaId).filterEqual(QApartmentLayout.apartmentName, roomType).filterEqual(QApartmentLayout.status, EnabledStatus.Enabled));
            if (!CollectionUtils.isEmpty(apartmentLayoutList)) {
                apartmentLayout = apartmentLayoutList.get(0);
            } else {
                return new ImmutablePair<>(false, "房间户型未找到");
            }
        }
        String acreageStr = rowDatas.get(11);
        String useAcreageStr = rowDatas.get(12);
        Double acreage = 0D;
        Double useAcreage = 0D;
        if (StringUtils.isEmpty(acreageStr)) {
            try {
                acreage = Double.valueOf(acreageStr);
            } catch (Exception e) {
                return new ImmutablePair<>(false, "建筑面积数值转换失败");
            }
        }
        if (StringUtils.isEmpty(useAcreageStr)) {
            try {
                useAcreage = Double.valueOf(useAcreageStr);
            } catch (Exception e) {
                return new ImmutablePair<>(false, "使用面积数值转换失败");
            }
        }
        String occupancyTimeStr = rowDatas.get(13);
        String startChargingTimeStr = rowDatas.get(14);
        Date occupancyTime = null;
        Date startChargingTime = null;
        if (StringUtils.isNotEmpty(occupancyTimeStr)) {
            occupancyTime = dateConvert(occupancyTimeStr);
        }
        if (StringUtils.isNotEmpty(startChargingTimeStr)) {
            startChargingTime = dateConvert(occupancyTimeStr);
        }
        OccupancyStatus occupancyStatus = OccupancyStatus.OCCUPANCY;
        String occupancyStatusStr = rowDatas.get(15);
        if (StringUtils.isNotEmpty(occupancyStatusStr)) {
            if ("空置".equals(occupancyStatusStr)) {
                occupancyStatus = OccupancyStatus.EMPTY;
            }
        }
        DecorationStatus decorationStatus = DecorationStatus.ROUGH;
        String decorationStatusStr = rowDatas.get(16);
        if (StringUtils.isNotEmpty(decorationStatusStr)) {
            if ("简装".equals(decorationStatusStr)) {
                decorationStatus = DecorationStatus.SIMPLE;
            } else if ("精装".equals(decorationStatusStr)) {
                decorationStatus = DecorationStatus.HARDCOVER;
            }
        }
        DataImportHouseInfo dataImportHouseInfo = new DataImportHouseInfo();
        dataImportHouseInfo.setAreaId(areaId.intValue());
        dataImportHouseInfo.setDataImportId(dataImportId);
        dataImportHouseInfo.setManageAreaId(managerArea.getId());
        dataImportHouseInfo.setManagerAreaName(managerArea.getAreaName());
        dataImportHouseInfo.setFloorId(floorInfo.getId());
        dataImportHouseInfo.setFloorNo(floorInfo.getFloorNo());
        dataImportHouseInfo.setUnitId(unitInfo.getId());
        dataImportHouseInfo.setUnitNo(unitInfo.getUnitNo());
        dataImportHouseInfo.setRoomId(roomId);
        dataImportHouseInfo.setRoomName(roomName);
        if (apartmentLayout == null) {
            dataImportHouseInfo.setLayoutId(0);
        } else {
            dataImportHouseInfo.setLayoutId(apartmentLayout.getId().intValue());
        }
        dataImportHouseInfo.setLayerId(floorLayerInfo.getId());
        dataImportHouseInfo.setAcreage(acreage);
        dataImportHouseInfo.setUsedAcreage(useAcreage);
        dataImportHouseInfo.setOccupancyStatus(occupancyStatus);
        dataImportHouseInfo.setDecorationStatus(decorationStatus);
        dataImportHouseInfo.setOccupancyTime(occupancyTime);
        dataImportHouseInfo.setStartChargingTime(startChargingTime);
        dataImportHouseInfo.setRoomAddress(String.format("%s-%s%s-%s%s-%s%s-%s%s", managerArea.getAreaName(), floorInfo.getFloorNo(), floorInfo.getFloorName(), unitInfo.getUnitNo(), unitInfo.getUnitName(), floorLayerInfo.getNumber(), floorLayerInfo.getName(), dataImportHouseInfo.getRoomId(), dataImportHouseInfo.getRoomName()));
        dataImportApiService.saveHouseInfo(dataImportHouseInfo);
        return new ImmutablePair<>(true, "");
    }

    private Pair<Boolean, String> preHousehold(List<List<String>> datas, Long areaId, Long dataImportId) throws Exception {
        if (datas == null || datas.size() != 1) {
            return new ImmutablePair<>(false, "未读取到数据");
        }
        List<String> rowDatas = datas.get(0);
        if (rowDatas.size() < 10) {
            return new ImmutablePair<>(false, "数据列数不符合");
        }
        String areaName = rowDatas.get(0);
        List<AreaInfo> areaInfoList = smartAreaInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QAreaInfo.areaName, areaName));
        if (areaInfoList == null || areaInfoList.size() < 1 || !areaInfoList.get(0).getId().equals(areaId)) {
            return new ImmutablePair<>(false, "未找到小区信息");
        }
        String managerName = rowDatas.get(1);
        List<ManagerArea> managerAreaList = managerAreaApiService.findAll(ApiRequest.newInstance().filterEqual(QManagerArea.areaId, areaId).filterEqual(QManagerArea.areaName, managerName).filterEqual(QManagerArea.status, EnabledStatus.Enabled));
        if (managerAreaList == null || managerAreaList.size() < 1) {
            return new ImmutablePair<>(false, "未找到小区项目信息");
        }
        ManagerArea managerArea = managerAreaList.get(0);
        String floorNo = rowDatas.get(2);
        List<FloorInfo> floorInfoList = smartFloorInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QFloorInfo.manageAreaId, managerArea.getId()).filterEqual(QFloorInfo.floorNo, floorNo).filterEqual(QFloorInfo.enabledStatus, EnabledStatus.Enabled));
        if (floorInfoList == null || floorInfoList.size() < 1) {
            return new ImmutablePair<>(false, "未找到小区楼宇信息");
        }
        FloorInfo floorInfo = floorInfoList.get(0);
        String unitNo = rowDatas.get(3);
        List<FloorUnitInfo> floorUnitInfoList = floorUnitInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QFloorUnitInfo.floorId, floorInfo.getId()).filterEqual(QFloorUnitInfo.unitNo, unitNo).filterEqual(QFloorUnitInfo.enabledStatus, EnabledStatus.Enabled));
        if (floorUnitInfoList == null || floorUnitInfoList.size() < 1) {
            return new ImmutablePair<>(false, "未找到小区单元信息");
        }
        FloorUnitInfo unitInfo = floorUnitInfoList.get(0);
        String roomId = rowDatas.get(4);
        List<HouseInfo> houseInfoList = smartHouseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseInfo.unitId, unitInfo.getId()).filterEqual(QHouseInfo.roomId, roomId).filterEqual(QHouseInfo.enabledStatus, EnabledStatus.Enabled));
        if (floorUnitInfoList == null || floorUnitInfoList.size() < 1) {
            return new ImmutablePair<>(false, "未找到房间信息");
        }
        HouseInfo houseInfo = houseInfoList.get(0);
        String name = rowDatas.get(5);
        if (StringUtils.isEmpty(name)) {
            return new ImmutablePair<>(false, "住户姓名为空");
        }
        String telephone = rowDatas.get(6);
        if (StringUtils.isEmpty(telephone)) {
            return new ImmutablePair<>(false, "住户手机号为空");
        }
        String typeStr = rowDatas.get(7);
        if (StringUtils.isEmpty(typeStr)) {
            return new ImmutablePair<>(false, "住户类型为空");
        }
        Identity identity = Identity.resident_others;
        if ("租户".equals(typeStr)) {
            identity = Identity.resident_renter;
        } else if ("业主亲戚".equals(typeStr)) {
            identity = Identity.resident_relative;
        } else if ("业主".equals(typeStr)) {
            identity = Identity.resident_owner;
        }
        String sexStr = rowDatas.get(8);
        if (StringUtils.isEmpty(typeStr)) {
            return new ImmutablePair<>(false, "住户性别不能为空");
        }
        Gender gender = Gender.Unknown;
        if ("男".equals(sexStr)) {
            gender = Gender.Male;
        } else if ("女".equals(sexStr)) {
            gender = Gender.Female;
        }
        String isLiving = rowDatas.get(9);
        if (StringUtils.isEmpty(isLiving)) {
            return new ImmutablePair<>(false, "在住情况不能为空");
        }
        Boolean isLiv = false;
        if ("在住".equals(typeStr)) {
            isLiv = true;
        } else if ("不在住".equals(typeStr)) {
            isLiv = false;
        } else {
            return new ImmutablePair<>(false, "在住信息不对");
        }
        DataImportHouseholdsInfo dataImportHouseholdsInfo = new DataImportHouseholdsInfo();
        dataImportHouseholdsInfo.setDataImportId(dataImportId);
        dataImportHouseholdsInfo.setAreaId(areaId.intValue());
        dataImportHouseholdsInfo.setHouseId(houseInfo.getId());
        dataImportHouseholdsInfo.setManageAreaId(managerArea.getId());
        dataImportHouseholdsInfo.setFloorId(floorInfo.getId());
        dataImportHouseholdsInfo.setUnitId(unitInfo.getId());
        dataImportHouseholdsInfo.setGender(gender);
        dataImportHouseholdsInfo.setIslivein(isLiv);
        dataImportHouseholdsInfo.setIdentity(identity);
        dataImportApiService.saveHouseholdInfo(dataImportHouseholdsInfo);
        return new ImmutablePair<>(true, "");
    }


    @Override
    public String getConsumerId() {
        return "data_import";
    }

    private Date dateConvert(String str) throws Exception {
        String[] dates = str.split("/");
        if (dates.length != 3) {
            throw new Exception("日期转换错误");
        }
        Date date = new Date();
        DateUtils.setYears(date, Integer.valueOf(dates[0]));
        date = DateUtils.setMonths(date, Integer.valueOf(dates[1]));
        date = DateUtils.setDays(date, Integer.valueOf(dates[2]));
        date = DateUtils.setHours(date, 0);
        date = DateUtils.setMinutes(date, 0);
        date = DateUtils.setSeconds(date, 0);
        date = DateUtils.setMilliseconds(date, 0);
        return date;
    }
}
