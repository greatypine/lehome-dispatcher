package cn.lehome.dispatcher.queue.service.impl.house;

import cn.lehome.base.pro.api.bean.address.AddressBaseInfo;
import cn.lehome.base.pro.api.bean.address.QAddressBaseInfo;
import cn.lehome.base.pro.api.bean.house.AddressBean;
import cn.lehome.base.pro.api.bean.house.FloorInfo;
import cn.lehome.base.pro.api.bean.house.HouseInfo;
import cn.lehome.base.pro.api.bean.house.QHouseInfo;
import cn.lehome.base.pro.api.service.address.AddressBaseApiService;
import cn.lehome.base.pro.api.service.house.FloorInfoApiService;
import cn.lehome.base.pro.api.service.house.HouseInfoApiService;
import cn.lehome.bean.pro.enums.address.ExtendType;
import cn.lehome.dispatcher.queue.service.house.AddressChangeService;
import cn.lehome.dispatcher.queue.service.impl.AbstractBaseServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
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
            addressBean.setFloorName(floorInfo.getFloorName());
            addressBean.setFloorNumber(floorInfo.getFloorNo());
            info.setAddress(JSON.toJSONString(addressBean));
        }
        if (!CollectionUtils.isEmpty(updateList)) {
            addressBaseApiService.batchSave(updateList);
        }
    }

    private void changeHouse(FloorInfo floorInfo) {
        List<HouseInfo> list = houseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseInfo.floorId, floorInfo.getId()));
        for (HouseInfo houseInfo : list) {
            houseInfo.setFloorNo(floorInfo.getFloorNo());
            houseInfo.setFloorName(floorInfo.getFloorName());
            houseInfoApiService.update(houseInfo);
        }
    }
}
