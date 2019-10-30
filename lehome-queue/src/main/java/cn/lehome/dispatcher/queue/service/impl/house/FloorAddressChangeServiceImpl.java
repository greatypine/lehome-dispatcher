package cn.lehome.dispatcher.queue.service.impl.house;

import cn.lehome.base.pro.api.bean.address.AddressBaseInfo;
import cn.lehome.base.pro.api.bean.address.QAddressBaseInfo;
import cn.lehome.base.pro.api.bean.address.AddressBean;
import cn.lehome.base.pro.api.bean.area.ManagerArea;
import cn.lehome.base.pro.api.bean.house.*;
import cn.lehome.base.pro.api.service.address.AddressBaseApiService;
import cn.lehome.base.pro.api.service.area.ManagerAreaApiService;
import cn.lehome.base.pro.api.service.house.FloorInfoApiService;
import cn.lehome.base.pro.api.service.house.FloorLayerInfoApiService;
import cn.lehome.base.pro.api.service.house.FloorUnitInfoApiService;
import cn.lehome.base.pro.api.service.house.HouseInfoApiService;
import cn.lehome.bean.pro.enums.address.ExtendType;
import cn.lehome.dispatcher.queue.service.house.AddressChangeService;
import cn.lehome.dispatcher.queue.service.impl.AbstractBaseServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2019/5/31.
 */
@Service("floorAddressChangeService")
public class FloorAddressChangeServiceImpl extends AbstractBaseServiceImpl implements AddressChangeService {

    @Autowired
    private FloorInfoApiService floorInfoApiService;

    @Autowired
    private AddressBaseApiService addressBaseApiService;

    @Autowired
    private HouseInfoApiService houseInfoApiService;

    @Autowired
    private FloorLayerInfoApiService floorLayerInfoApiService;

    @Autowired
    private ManagerAreaApiService managerAreaApiService;

    @Autowired
    private FloorUnitInfoApiService floorUnitInfoApiService;

    @Override
    public void changeName(Integer id) {
        FloorInfo floorInfo = floorInfoApiService.findOne(id);
        if (floorInfo == null) {
            logger.error("楼宇未找到, id = " + id);
            return;
        }
        this.changeHouse(floorInfo);
        AddressBaseInfo addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.BUILDING, id.longValue());
        if (addressBaseInfo == null) {
            logger.error("楼宇地址未找到, id = " + id);
            return;
        }
        List<AddressBaseInfo> updateList = Lists.newArrayList();
        updateList.add(addressBaseInfo);
        List<AddressBaseInfo> unitAddressList = addressBaseApiService.findAll(ApiRequest.newInstance().filterEqual(QAddressBaseInfo.parentId, addressBaseInfo.getId()));
        if (!CollectionUtils.isEmpty(unitAddressList)) {
            updateList.addAll(unitAddressList);
            List<AddressBaseInfo> houseAddressList = addressBaseApiService.findAll(ApiRequest.newInstance().filterIn(QAddressBaseInfo.parentId, unitAddressList.stream().map(AddressBaseInfo::getId).collect(Collectors.toList())));
            if (!CollectionUtils.isEmpty(houseAddressList)) {
                updateList.addAll(houseAddressList);
            }
        }

        for (AddressBaseInfo info : updateList) {
            AddressBean addressBean = JSON.parseObject(info.getAddress(), AddressBean.class);
            addressBean.setBuildingNumber(floorInfo.getFloorName());
            addressBean.setBuildingName(floorInfo.getFloorNo());
            info.setAddress(JSON.toJSONString(addressBean));
        }
        if (!CollectionUtils.isEmpty(updateList)) {
            List<AddressBaseInfo> tempList = Lists.newArrayList();
            for (AddressBaseInfo addressBaseInfo1 : updateList) {
                tempList.add(addressBaseInfo1);
                if (tempList.size() > 30) {
                    addressBaseApiService.batchSave(tempList);
                    tempList.clear();
                }
            }
            if (!CollectionUtils.isEmpty(tempList)) {
                addressBaseApiService.batchSave(tempList);
            }
        }
    }

    private void changeHouse(FloorInfo floorInfo) {
        List<HouseInfo> list = houseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseInfo.floorId, floorInfo.getId()));
        ManagerArea managerArea = managerAreaApiService.findOne(floorInfo.getManageAreaId());
        if (managerArea == null) {
            logger.error("管控区域未找到, managerAreaId = " + floorInfo.getManageAreaId());
            return;
        }
        for (HouseInfo houseInfo : list) {
            houseInfo.setFloorName(floorInfo.getFloorName());
            houseInfo.setFloorNo(floorInfo.getFloorNo());
            String roomAddress = managerArea.getAreaName();
            if (StringUtils.isNotEmpty(houseInfo.getFloorNo())) {
                roomAddress += "-" + houseInfo.getFloorNo();
                if (StringUtils.isNotEmpty(floorInfo.getFloorName())) {
                    roomAddress += floorInfo.getFloorName();
                }
            }
            if (StringUtils.isNotEmpty(houseInfo.getUnitNo())) {
                roomAddress += "-" + houseInfo.getUnitNo();
                FloorUnitInfo floorUnitInfo = floorUnitInfoApiService.findOne(houseInfo.getUnitId());
                if (floorUnitInfo != null && StringUtils.isNotEmpty(floorUnitInfo.getUnitName())) {
                    roomAddress += floorUnitInfo.getUnitName();
                }
            }
            roomAddress += "-" + houseInfo.getRoomId();
            if (StringUtils.isNotEmpty(houseInfo.getRoomName())) {
                roomAddress += houseInfo.getRoomName();
            }
            houseInfo.setRoomAddress(roomAddress);
            houseInfoApiService.update(houseInfo);
        }
    }
}
