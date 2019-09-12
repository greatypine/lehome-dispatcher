package cn.lehome.dispatcher.utils.smart;

import cn.lehome.base.api.old.pro.bean.area.OldManagerArea;
import cn.lehome.base.api.old.pro.bean.area.QOldManagerArea;
import cn.lehome.base.api.old.pro.bean.facility.OldExtendFacility;
import cn.lehome.base.api.old.pro.bean.facility.OldExtendFacilityRelation;
import cn.lehome.base.api.old.pro.bean.facility.QOldExtendFacility;
import cn.lehome.base.api.old.pro.bean.facility.QOldExtendFacilityRelation;
import cn.lehome.base.api.old.pro.bean.house.*;
import cn.lehome.base.api.old.pro.bean.household.OldHouseholdsInfo;
import cn.lehome.base.api.old.pro.bean.household.OldHouseholdsSettingsInfo;
import cn.lehome.base.api.old.pro.service.area.OldManagerAreaApiService;
import cn.lehome.base.api.old.pro.service.facility.OldExtendFacilityApiService;
import cn.lehome.base.api.old.pro.service.house.OldFloorInfoApiService;
import cn.lehome.base.api.old.pro.service.house.OldFloorUnitInfoApiService;
import cn.lehome.base.api.old.pro.service.house.OldHouseInfoApiService;
import cn.lehome.base.api.old.pro.service.household.OldHouseholdsInfoApiService;
import cn.lehome.bean.pro.old.enums.EnabledStatus;
import cn.lehome.bean.pro.old.enums.RecordDeleteStatus;
import cn.lehome.bean.pro.old.enums.facility.EntranceGuardType;
import cn.lehome.bean.pro.old.enums.house.HouseState;
import cn.lehome.dispatcher.utils.smart.bean.RpcRequest;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.util.CoreHttpUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by zuoguodong on 2019/9/4
 */
@Service("smartAreaImportService")
public class SmartAreaImportServiceImpl implements SmartAreaImportService{
    private static final Logger logger = LoggerFactory.getLogger(SmartAreaImportServiceImpl.class);
    private static final String DOMAIN = "http://smart.sqbj.com";
    private static final String baseUrl = DOMAIN.concat("/api/basic/json-rpc/views");
    private static final String savefacilityUrl = DOMAIN.concat("/pro_app_api/control_regions");

    private static final String authorization = "bearer eyJhbGciOiJSUzI1NiIsImtpZCI6Ijk5ZGYxYzAyNTk4YWVkZGY1ZjQ1M2JlZTQ4NzMyMmNlIiwidHlwIjoiSldUIn0.eyJhdWQiOiI5OWRmMWMwMjU5OGFlZGRmNWY0NTNiZWU0ODczMjJjZSIsImV4cCI6MTU2ODMwOTI2OCwiaWF0IjoxNTY4MjY2MDY4LCJpc3MiOiJiYnAtdXNlcnMiLCJqdGkiOiIzMTlhNzU4OTQ1NWI0Y2Q0ZTBjODJlYzBmMDFjZjk4MyIsInN1YiI6ImZjY2ExMmIwY2JiMDgzYmNjNDVlNTlmZWE1ZWJlMjU1IiwidWdpIjoiZjNiZjg4ZmFkYzdiMzI0YjNiNTYxZmUzOWZlYTU2ZTciLCJ1dGkiOiJmM2JmODhmYWRjN2IzMjRiM2I1NjFmZTM5ZmVhNTZlNyJ9.dLa_r5iehiKsscyKZcReZ4mYzzo3WOfYxuYIs9uKnhjJs2vXTzdcrcatOuzwjRi1GEywptaJ4ShClE6JbHEiUzjTdBh4UrnO2iiyam1uQ33UR8Nx6f3UKprTTbXF538XAHGGKa4T42CSZRazKDIQbQEdT-5KzJVyFudNc7XiMf9vKR5IAevNiJAO9AQFphXBA8K8XxqNAXA3q-ReJbpW03e6SPidrMbDF2HzWEJ3CNYFtT3Kz06sBovQtaSaWgnqfGQDtpjicz-aesRNgyb9Mdm91nlRJXA2b4fYdNGRF2uKbMeSiLeQ6OAOgIDfwXObMYvAf9XRmYD0KHCbMJ3KpQ";
    private static final String x_user_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1NjgzMDM3NTcsImlhdCI6MTU2ODI2MDU1NywiaXNzIjoiYmJwLXVzZXJzIiwianRpIjoiNWYwMWU2OTg4YzJmNTE5OWFlZjAyNzI1MmI1ZjkwNDAiLCJzdWIiOiIwNGYyYmY0MWU5YjNkZmMzYzY3NjExNWY0ZThlYzY1ZCIsInV0aSI6ImYzYmY4OGZhZGM3YjMyNGIzYjU2MWZlMzlmZWE1NmU3In0.hTWn8EURoPXWJXu6nj6VBF4PGlqQOo8gr7c2kf_F_c8";

    private Long oldAreaId;
    private Long newAreaId;
    private Long houseTypeId;



    private boolean importBase;


    @Autowired
    OldManagerAreaApiService oldManagerAreaApiService;

    @Autowired
    OldFloorInfoApiService oldFloorInfoApiService;

    @Autowired
    OldFloorUnitInfoApiService oldFloorUnitInfoApiService;

    @Autowired
    OldHouseInfoApiService oldHouseInfoApiService;

    @Autowired
    OldHouseholdsInfoApiService oldHouseholdsInfoApiService;

    @Autowired
    OldExtendFacilityApiService oldExtendFacilityApiService;

    private Map<Long,Long> projectCache = new HashMap<>();
    private Map<Long,Long> buildingCache = new HashMap<>();
    private Map<Long,Long> unitCache = new HashMap<>();

    @Override
    public void importData(String[] input) {
        List<Map<String,Long>> areaData = getAreaData();
        for(Map<String,Long> data : areaData) {
            try {
                projectCache.clear();
                buildingCache.clear();
                unitCache.clear();
                oldAreaId = data.get("oldAreaId");
                newAreaId = data.get("newAreaId");
                houseTypeId = data.get("houseTypeId");
                importBase = true;
                log("开始导入基础数据");
                importManagerAreaInfo();
                log("基础数据导入完成，开始导入管控区域");
                importExtendFacility();
                log("管控区域导入完成，开始导入房产、用户数据");
                importBase = false;
                importManagerAreaInfo();
                log("数据导入完成");
            }catch(Exception e){
                error("数据导入出错",e,data.get("oldAreaId"));
            }
        }
    }

    private List<Map<String,Long>> getAreaData(){
        List<Map<String,Long>> result = new ArrayList<>();

        Map<String,Long> d1 = new HashMap<>();
        d1.put("houseTypeId",22569L);
        d1.put("oldAreaId",292L);
        d1.put("newAreaId",11798L);
        result.add(d1);

        Map<String,Long> d2 = new HashMap<>();
        d2.put("houseTypeId",22573L);
        d2.put("oldAreaId",463L);
        d2.put("newAreaId",11803L);
        result.add(d2);

        Map<String,Long> d3 = new HashMap<>();
        d3.put("houseTypeId",22574L);
        d3.put("oldAreaId",303L);
        d3.put("newAreaId",11810L);
        result.add(d3);

        Map<String,Long> d4 = new HashMap<>();
        d4.put("houseTypeId",22604L);
        d4.put("oldAreaId",302L);
        d4.put("newAreaId",11817L);
        result.add(d4);

        Map<String,Long> d5 = new HashMap<>();
        d5.put("houseTypeId",22605L);
        d5.put("oldAreaId",297L);
        d5.put("newAreaId",11811L);
        result.add(d5);

        Map<String,Long> d6 = new HashMap<>();
        d6.put("houseTypeId",22605L);
        d6.put("oldAreaId",295L);
        d6.put("newAreaId",11811L);
        result.add(d6);

        Map<String,Long> d7 = new HashMap<>();
        d7.put("houseTypeId",22605L);
        d7.put("oldAreaId",296L);
        d7.put("newAreaId",11811L);
        result.add(d7);

        return result;
    }

    private void importExtendFacility(){
        List<OldExtendFacility> list = oldExtendFacilityApiService.findFacilityAll(ApiRequest.newInstance().filterEqual(QOldExtendFacility.areaId,oldAreaId).filterEqual(QOldExtendFacility.status, EnabledStatus.Enabled));
        list.forEach(oldExtendFacility -> {
            List<OldExtendFacilityRelation> relations = oldExtendFacilityApiService.findAll(ApiRequest.newInstance().filterEqual(QOldExtendFacilityRelation.facilityId,oldExtendFacility.getId()).filterEqual(QOldExtendFacilityRelation.deleteStatus, RecordDeleteStatus.Normal));
            saveExtendFacilityRequest(oldExtendFacility,relations);
        });
    }

    private void saveExtendFacilityRequest(OldExtendFacility oldExtendFacility,List<OldExtendFacilityRelation> relations){
        try {
            Map<String, Object> params = new HashMap<>();
            params.put("name", oldExtendFacility.getName());
            params.put("control_type", "ENTRANCE_GUARD");
            params.put("auto_authorize", "YES");
            params.put("through_type", "PEOPLE");
            params.put("through_direction", "ENTRANCE_AND_EXIT");
            if(oldExtendFacility.getCategory().equals(EntranceGuardType.AreaDoor)) {
                List<Map<String,Long>> result = new ArrayList<>();
                projectCache.values().forEach(newProjectId -> {
                    Map<String,Long> map = new HashMap<>();
                    map.put("community_id",newAreaId);
                    map.put("project_id",newProjectId);
                    result.add(map);
                });
                params.put("positions", result);
            }else{
                params.put("positions", getPositions(relations));
            }
            String response = CoreHttpUtils.post(savefacilityUrl, getHeader(), JSON.toJSONString(params), String.class).getResponse();
            log("保存管控区域", oldExtendFacility.getName(), response);
        }catch(Exception e){
           error("保存管控区域时出错",e,oldExtendFacility.getName());
        }
    }

    private List<Map<String,Long>> getPositions(List<OldExtendFacilityRelation> relations){
        List<Map<String,Long>> result = new ArrayList<>();
        for(OldExtendFacilityRelation oldExtendFacilityRelation : relations){
            if(projectCache.get(oldExtendFacilityRelation.getManagerId().longValue()) == null){
                continue;
            }
            Map<String,Long> map = new HashMap<>();
            map.put("community_id",newAreaId);
            map.put("project_id",projectCache.get(oldExtendFacilityRelation.getManagerId().longValue()));
            map.put("floor_id",buildingCache.get(oldExtendFacilityRelation.getFloorId().longValue()));
            map.put("unit_id",unitCache.get(oldExtendFacilityRelation.getUnitId().longValue()));
            result.add(map);
        }
        return result;
    }

    private void importManagerAreaInfo(){
        log("跟据小区查找原项目");
        List<OldManagerArea> oldManagerAreas = oldManagerAreaApiService.findAll(ApiRequest.newInstance().filterEqual(QOldManagerArea.areaId,oldAreaId));
        for(OldManagerArea oldManagerArea : oldManagerAreas){
            if(oldManagerArea.getAreaName().contains("删除")){
                continue;
            }
            if(importBase) {
                saveManagerAreaInfo(oldManagerArea.getAreaName());
            }
            log("获取新项目ID");
            Long projectId = findManagerAreaInfo(oldManagerArea.getAreaName());
            projectCache.put(oldManagerArea.getId().longValue(),projectId);
            log("查找原始楼宇列表",projectId);
            List<OldFloorInfo> oldFloorInfos = oldFloorInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QOldFloorInfo.manageAreaId,oldManagerArea.getId()));
            oldFloorInfos.forEach(oldFloorInfo -> {
                if(importBase) {
                    saveFloorInfo(projectId, oldFloorInfo.getFloorNo());
                }
                log("获取新楼宇ID");
                Long buildingId = findFloorInfo(projectId,oldFloorInfo.getFloorNo());
                buildingCache.put(oldFloorInfo.getId().longValue(),buildingId);
                log("查找原始单元列表",buildingId);
                List<OldFloorUnitInfo> oldFloorUnitInfos = oldFloorUnitInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QOldFloorUnitInfo.floorId,oldFloorInfo.getId()));
                oldFloorUnitInfos.forEach(oldFloorUnitInfo -> {
                    if(importBase) {
                        saveUnitInfo(buildingId, oldFloorUnitInfo);
                    }
                    log("获取新单元ID");
                    Long unitId = findUnitInfo(buildingId,oldFloorUnitInfo.getUnitNo());
                    unitCache.put(oldFloorUnitInfo.getId().longValue(),unitId);
                    if(!importBase) {
                        log("查找原始房屋列表", unitId);
                        List<OldHouseInfo> oldHouseInfos = oldHouseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QOldHouseInfo.unitId, oldFloorUnitInfo.getId()));
                        oldHouseInfos.forEach(oldHouseInfo -> {
                            saveHouseInfo(projectId, buildingId, unitId, oldHouseInfo);
                            log("获取新的房屋ID");
                            Long houseId = findHouseInfo(projectId, buildingId, unitId, oldHouseInfo.getRoomId());
                            log("查找原始人员信息", houseId);
                            List<OldHouseholdsInfo> oldHouseholdsInfos = oldHouseholdsInfoApiService.findByHouseId(oldHouseInfo.getId(), null);
                            oldHouseholdsInfos.forEach(oldHouseholdsInfo -> {
                                saveOldhouseHoldsInfo(projectId, buildingId, unitId, houseId, oldHouseholdsInfo);
                            });
                        });
                    }
                });
            });
        }
    }

    private void saveOldhouseHoldsInfo(Long projectId,Long buildingId,Long unitId,Long houseId,OldHouseholdsInfo oldHouseholdsInfo){
        try {
            RpcRequest request = new RpcRequest();
            request.setMethod("GP:MAINWEBB:ResidentCreate:CreateResident");
            Map<String, Object> params = new HashMap<>();
            params.put("areaId", projectId);
            params.put("buildingId", buildingId);
            params.put("unitId", unitId);
            params.put("houseId", houseId);
            params.put("name", oldHouseholdsInfo.getName());
            params.put("phone", oldHouseholdsInfo.getTelephone());
            params.put("birthday", oldHouseholdsInfo.getBirthday());
            params.put("nation", oldHouseholdsInfo.getNation());
            params.put("ethnicity", oldHouseholdsInfo.getNation());
            params.put("idcard", oldHouseholdsInfo.getIdentityCard());
            params.put("workplace", oldHouseholdsInfo.getCompany());
            params.put("email", oldHouseholdsInfo.getEmail());
            params.put("sparePhone", oldHouseholdsInfo.getSpareTelephone());
            params.put("islivein", true);
            params.put("sex", oldHouseholdsInfo.getGender());
            params.put("identity", getIdentify(oldHouseholdsInfo.getId()));
            request.getParams().add(params);
            String response = CoreHttpUtils.post(baseUrl, getHeader(), JSON.toJSONString(request), String.class).getResponse();
            log("保存人员", projectId, buildingId, unitId, oldHouseholdsInfo.getName(), response);
        }catch(Exception e){
            error("保存人员时出错",e,projectId,buildingId,unitId,houseId,oldHouseholdsInfo.getName());
        }
    }

    private String getIdentify(Integer houseId){
        OldHouseholdsSettingsInfo oldHouseholdsSettingsInfo = oldHouseholdsInfoApiService.findSettingByHouseholdId(houseId);
        Integer typeId = oldHouseholdsSettingsInfo.getHouseholdsTypeId();
        switch (typeId) {
            case 1:
                return "resident_owner";
            case 5:
                return "resident_relative";
            case 6:
                return "resident_renter";
            default:
                return "resident_others";
        }
    }

    private Long findHouseInfo(Long projectId,Long buildingId,Long unitId, String roomId){
        try {
            RpcRequest request = new RpcRequest();
            request.setMethod("GP:MAINWEBB:HouseList:GetPage");
            request.getParams().add(newAreaId);
            request.getParams().add(projectId);
            request.getParams().add(buildingId);
            request.getParams().add(unitId);
            request.getParams().add("");
            request.getParams().add(null);
            request.getParams().add(null);
            request.getParams().add(null);
            request.getParams().add(0);
            request.getParams().add(2000);
            request.getParams().add("");
            String response = CoreHttpUtils.post(baseUrl, getHeader(), JSON.toJSONString(request), String.class).getResponse();
            JSONObject jsonObject = JSON.parseObject(response);
            JSONArray jsonArray = jsonObject.getJSONObject("result").getJSONObject("houses").getJSONArray("items");
            for (Object o : jsonArray) {
                JSONObject project = (JSONObject) o;
                if (project.get("number").toString().equals(roomId)) {
                    return Long.valueOf(project.get("id").toString());
                }
            }
        }catch(Exception e){
            error("获取房屋信息时出错",e,projectId,buildingId,unitId,roomId);
        }
        return null;
    }

    private void saveHouseInfo(Long projectId,Long buildingId,Long unitId,OldHouseInfo oldHouseInfo){
        try {
            RpcRequest request = new RpcRequest();
            request.setMethod("GP:MAINWEBB:HouseCreate:CreateHouse");
            Map<String, Object> params = new HashMap<>();
            params.put("buildingId", buildingId);
            params.put("projectId", projectId);
            params.put("unitId", unitId);
            params.put("number", oldHouseInfo.getRoomId());
            params.put("name", "室");
            params.put("grossArea", oldHouseInfo.getAcreage());
            params.put("netArea", oldHouseInfo.getUsedAcreage());
            params.put("handoverDate", null);
            params.put("startChargingTime", oldHouseInfo.getStartChargingTime());
            params.put("moveinState", oldHouseInfo.getHouseState().equals(HouseState.Occupancy) ? "OCCUPANCY" : "EMPTY");
            params.put("decorateState", "");
            params.put("houseTypeId", houseTypeId);
            params.put("houseTypeName", "默认户型");
            params.put("floorId", getLayerId(buildingId, unitId, oldHouseInfo.getLayerId()));
            params.put("builtupArea", oldHouseInfo.getAcreage());
            params.put("dwellingareaArea", oldHouseInfo.getUsedAcreage());
            request.getParams().add(params);
            String response = CoreHttpUtils.post(baseUrl, getHeader(), JSON.toJSONString(request), String.class).getResponse();
            log("保存房屋", projectId, buildingId, unitId, oldHouseInfo.getRoomId(), response);
        }catch(Exception e){
            error("保存房层时出错",e,projectId,buildingId,unitId,oldHouseInfo.getRoomId());
        }
    }

    private Long getLayerId(Long buildingId,Long unitId,Integer layerNo){
        try {
            RpcRequest request = new RpcRequest();
            request.setMethod("GP:MAINWEBB:BuildingSetting:GetUnits");
            request.getParams().add(buildingId);
            String response = CoreHttpUtils.post(baseUrl, getHeader(), JSON.toJSONString(request), String.class).getResponse();
            JSONObject jsonObject = JSON.parseObject(response);
            JSONArray jsonArray = jsonObject.getJSONObject("result").getJSONArray("units");
            for (Object o : jsonArray) {
                JSONObject project = (JSONObject) o;
                if (Long.valueOf(project.get("id").toString()).equals(unitId)) {
                    JSONArray floors = project.getJSONArray("floors");
                    for (Object o1 : floors) {
                        JSONObject floor = (JSONObject) o1;
                        if (layerNo.equals(floor.get("number"))) {
                            return Long.valueOf(floor.get("id").toString());
                        }
                    }
                }
            }
        }catch(Exception e){
            error("获取楼层ID时出错",e,buildingId,unitId,layerNo);
        }
        return null;
    }

    private Long findUnitInfo(Long buildingId,String unitNo){
        try {
            RpcRequest request = new RpcRequest();
            request.setMethod("GP:MAINWEBB:BuildingSetting:GetUnits");
            request.getParams().add(buildingId);
            String response = CoreHttpUtils.post(baseUrl, getHeader(), JSON.toJSONString(request), String.class).getResponse();
            JSONObject jsonObject = JSON.parseObject(response);
            JSONArray jsonArray = jsonObject.getJSONObject("result").getJSONArray("units");
            for (Object o : jsonArray) {
                JSONObject project = (JSONObject) o;
                if (unitNo.equals(project.get("number").toString() + project.get("name").toString())) {
                    return Long.valueOf(project.get("id").toString());
                }
            }
        }catch(Exception e){
            error("查找单元时出错",e,buildingId,unitNo);
        }
        return null;
    }

    private Long findFloorInfo(Long projectId,String floorNo){
        try {
            RpcRequest request = new RpcRequest();
            request.setMethod("GP:MAINWEBB:BuildingSetting:GetPage");
            request.getParams().add(newAreaId);
            request.getParams().add(projectId);
            String response = CoreHttpUtils.post(baseUrl, getHeader(), JSON.toJSONString(request), String.class).getResponse();
            JSONObject jsonObject = JSON.parseObject(response);
            JSONArray jsonArray = jsonObject.getJSONObject("result").getJSONArray("buildings");
            for (Object o : jsonArray) {
                JSONObject project = (JSONObject) o;
                if (floorNo.equals(project.get("number").toString() + project.get("name").toString())) {
                    return Long.valueOf(project.get("id").toString());
                }
            }
        }catch(Exception e){
            error("获取楼层时出错",e,projectId,floorNo);
        }
        return null;
    }

    private void saveUnitInfo(Long buildingId,OldFloorUnitInfo oldFloorUnitInfo){
        try {
            RpcRequest request = new RpcRequest();
            request.setMethod("GP:MAINWEBB:BuildingSetting:CreateUnit");
            Map<String, Object> params = new HashMap<>();
            params.put("buildingId", buildingId);
            params.put("type", "unit");
            params.put("number", oldFloorUnitInfo.getUnitNo());
            params.put("name", "");//因为把原始单元拆成数字和单位，智社区会出现重复的校验，所以原始不再拆分，直接做为number保存，单位段为空
            List<Map<String, String>> floorsList = getLayers(oldFloorUnitInfo.getId());
            params.put("floors", floorsList);
            request.getParams().add(params);
            String response = CoreHttpUtils.post(baseUrl, getHeader(), JSON.toJSONString(request), String.class).getResponse();
            log("保存单元", oldFloorUnitInfo.getUnitNo(), response);
        }catch(Exception e){
            error("保存单元时出错",e,buildingId,oldFloorUnitInfo.getUnitNo());
        }
    }

    private List<Map<String,String>> getLayers(Integer unitId){
        List<Map<String, String>> result = new ArrayList<>();
        try {
            List<OldHouseInfo> list = oldHouseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QOldHouseInfo.unitId, unitId));
            List<Integer> layerids = list.stream().map(OldHouseInfo::getLayerId).distinct().sorted().collect(Collectors.toList());
            layerids.forEach(id -> {
                Map<String, String> layer = new HashMap<>();
                layer.put("type", id > 0 ? "aboveground" : "underground");
                layer.put("number", id.toString());
                layer.put("name", id.toString());
                result.add(layer);
            });
        }catch(Exception e){
            error("根据单元获取楼层时出错",e,unitId);
        }
        return result;
    }

    private void saveFloorInfo(Long projectId,String floorNo){
        try {
            String number = getFloorNo(floorNo);
            String name = getFloorName(floorNo, number);
            RpcRequest request = new RpcRequest();
            request.setMethod("GP:MAINWEBB:BuildingSetting:CreateBuilding");
            Map<String, Object> params = new HashMap<>();
            params.put("projectId", projectId);
            params.put("type", "building");
            params.put("number", number);
            params.put("name", name);
            request.getParams().add(params);
            String response = CoreHttpUtils.post(baseUrl, getHeader(), JSON.toJSONString(request), String.class).getResponse();
            log("保存楼宇", floorNo, response);
        }catch(Exception e){
            error("保存楼宇时出错",e,projectId,floorNo);
        }
    }

    private Long findManagerAreaInfo(String areaName){
        try {
            RpcRequest request = new RpcRequest();
            request.setMethod("GP:MAINWEBB:BuildingSetting:GetPage");
            request.getParams().add(newAreaId);
            String response = CoreHttpUtils.post(baseUrl, getHeader(), JSON.toJSONString(request), String.class).getResponse();
            JSONObject jsonObject = JSON.parseObject(response);
            JSONArray jsonArray = jsonObject.getJSONObject("result").getJSONArray("projects");
            for (Object o : jsonArray) {
                JSONObject project = (JSONObject) o;
                if (areaName.equals(project.get("name"))) {
                    return Long.valueOf(project.get("id").toString());
                }
            }
        }catch(Exception e){
            error("根据名称查找项目时出错",e,areaName);
        }
        return null;
    }

    private String getFloorNo(String floorNo){
        for(int i = 0;i<floorNo.length();i++){
            try {
                Integer.valueOf(floorNo.substring(i, i + 1));
            }catch(Exception e){
                return floorNo.substring(0,i);
            }
        }
        return floorNo;
    }

    private String getFloorName(String floorNo,String number){
        return floorNo.substring(number.length());
    }

    private void saveManagerAreaInfo(String name){
        try {
            RpcRequest request = new RpcRequest();
            request.setMethod("GP:MAINWEBB:BuildingSetting:CreateProject");
            Map<String, Object> params = new HashMap<>();
            params.put("manageAreaId", newAreaId);
            params.put("type", "area");
            params.put("name", name);
            request.getParams().add(params);
            String response = CoreHttpUtils.post(baseUrl, getHeader(), JSON.toJSONString(request), String.class).getResponse();
            log("保存项目", name, response);
        }catch (Exception e){
            error("保存项目出错",e,name);
        }
    }



    private static Map<String,String> getHeader(){
        Map<String,String> header = new HashMap<>();
        header.put("Authorization",authorization);
        header.put("x-user-token",x_user_token);
        header.put("Content-Type","application/json");
        return header;
    }

    private void log(Object ... message){
        String log = "";
        for(Object m : message){
            if(m != null) {
                log = log.concat(m.toString()).concat(":");
            }
        }
        logger.info(log);
    }

    private void error(String bus,Exception e,Object ... message){
        StringBuffer log = new StringBuffer();
        log.append("===============================================================\n");
        log.append("业务:").append(bus).append("\n");
        log.append("原小区ID:").append(oldAreaId).append("\n");
        log.append("新小区ID:").append(newAreaId).append("\n");
        log.append("参数:");
        for(Object m : message){
            log.append(m.toString()).append(":");
        }
        log.append("\n");

        log.append("===============================================================\n");
        logger.error(log.toString(),e);
    }
}
