package cn.lehome.dispatcher.queue.service.impl.house;

import cn.lehome.base.pro.api.bean.address.AddressBaseInfo;
import cn.lehome.base.pro.api.bean.address.QAddressBaseInfo;
import cn.lehome.base.pro.api.bean.house.AddressBean;
import cn.lehome.base.pro.api.bean.house.FloorUnitInfo;
import cn.lehome.base.pro.api.bean.house.HouseInfo;
import cn.lehome.base.pro.api.bean.house.QHouseInfo;
import cn.lehome.base.pro.api.service.address.AddressBaseApiService;
import cn.lehome.base.pro.api.service.house.FloorUnitInfoApiService;
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

/**
 * Created by wuzhao on 2019/5/31.
 */
@Service("unitAddressChangeService")
public class UnitAddressChangeServiceImpl extends AbstractBaseServiceImpl implements AddressChangeService {
    @Autowired
    private FloorUnitInfoApiService floorUnitInfoApiService;

    @Autowired
    private AddressBaseApiService addressBaseApiService;

    @Autowired
    private HouseInfoApiService houseInfoApiService;

    @Override
    public void changeName(Integer id) {
        FloorUnitInfo floorUnitInfo = floorUnitInfoApiService.findOne(id);
        if (floorUnitInfo == null) {
            logger.error("单元未找到, id = " + id);
            return;
        }
        this.changeHouse(floorUnitInfo);
        AddressBaseInfo addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.UNIT, id.longValue());
        if (addressBaseInfo == null) {
            logger.error("单元地址未找到, id = " + id);
            return;
        }
        List<AddressBaseInfo> updateList = Lists.newArrayList();
        updateList.add(addressBaseInfo);
        List<AddressBaseInfo> houseAddressList = addressBaseApiService.findAll(ApiRequest.newInstance().filterEqual(QAddressBaseInfo.parentId, addressBaseInfo.getId()));
        if (!CollectionUtils.isEmpty(houseAddressList)) {
            updateList.addAll(houseAddressList);
        }

        for (AddressBaseInfo info : updateList) {
            AddressBean addressBean = JSON.parseObject(info.getAddress(), AddressBean.class);
            addressBean.setUnitName(floorUnitInfo.getUnitName());
            addressBean.setUnitNumber(floorUnitInfo.getUnitNo());
            info.setAddress(JSON.toJSONString(addressBean));
        }
        if (!CollectionUtils.isEmpty(updateList)) {
            addressBaseApiService.batchSave(updateList);
        }
    }

    private void changeHouse(FloorUnitInfo floorUnitInfo) {
        List<HouseInfo> list = houseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseInfo.unitId, floorUnitInfo.getId()));
        for (HouseInfo houseInfo : list) {
            houseInfo.setUnitNo(floorUnitInfo.getUnitNo());
            houseInfo.setUnitName(floorUnitInfo.getUnitName());
            houseInfoApiService.update(houseInfo);
        }
    }
}
