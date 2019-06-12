package cn.lehome.dispatcher.queue.service.impl.house;

import cn.lehome.base.pro.api.bean.address.AddressBaseInfo;
import cn.lehome.base.pro.api.bean.address.QAddressBaseInfo;
import cn.lehome.base.pro.api.bean.area.ManagerArea;
import cn.lehome.base.pro.api.bean.address.AddressBean;
import cn.lehome.base.pro.api.bean.house.HouseInfo;
import cn.lehome.base.pro.api.bean.house.QHouseInfo;
import cn.lehome.base.pro.api.service.address.AddressBaseApiService;
import cn.lehome.base.pro.api.service.area.ManagerAreaApiService;
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
@Service("managerAreaAddressChangeService")
public class ManagerAreaAddressChangeServiceImpl extends AbstractBaseServiceImpl implements AddressChangeService {

    @Autowired
    private ManagerAreaApiService managerAreaApiService;

    @Autowired
    private AddressBaseApiService addressBaseApiService;

    @Autowired
    private HouseInfoApiService houseInfoApiService;

    @Override
    public void changeName(Integer id) {
        ManagerArea managerArea = managerAreaApiService.findOne(id);
        if (managerArea == null) {
            logger.error("管理区域未找到, id = " + id);
            return;
        }
        this.changeHouse(managerArea);
        AddressBaseInfo addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.PROJECT, id.longValue());
        if (addressBaseInfo == null) {
            logger.error("管理区域地址未找到, id = " + id);
            return;
        }
        List<AddressBaseInfo> updateList = Lists.newArrayList();
        updateList.add(addressBaseInfo);
        List<AddressBaseInfo> floorAddressList = addressBaseApiService.findAll(ApiRequest.newInstance().filterEqual(QAddressBaseInfo.parentId, addressBaseInfo.getId()));
        if (!CollectionUtils.isEmpty(floorAddressList)) {
            updateList.addAll(floorAddressList);
            List<AddressBaseInfo> unitAddressList = addressBaseApiService.findAll(ApiRequest.newInstance().filterIn(QAddressBaseInfo.parentId, floorAddressList.stream().map(AddressBaseInfo::getId).collect(Collectors.toList())));
            if (!CollectionUtils.isEmpty(unitAddressList)) {
                updateList.addAll(unitAddressList);
                List<AddressBaseInfo> houseAddressList = addressBaseApiService.findAll(ApiRequest.newInstance().filterIn(QAddressBaseInfo.parentId, unitAddressList.stream().map(AddressBaseInfo::getId).collect(Collectors.toList())));
                if (!CollectionUtils.isEmpty(houseAddressList)) {
                    updateList.addAll(houseAddressList);
                }
            }
        }

        for (AddressBaseInfo info : updateList) {
            AddressBean addressBean = JSON.parseObject(info.getAddress(), AddressBean.class);
            addressBean.setProjectName(managerArea.getAreaName());
            info.setAddress(JSON.toJSONString(addressBean));
        }
        if (!CollectionUtils.isEmpty(updateList)) {
            addressBaseApiService.batchSave(updateList);
        }
    }

    private void changeHouse(ManagerArea managerArea) {
        List<HouseInfo> list = houseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseInfo.manageAreaId, managerArea.getId()));
        for (HouseInfo houseInfo : list) {
            houseInfo.setManagerAreaName(managerArea.getAreaName());
            houseInfoApiService.update(houseInfo);
        }
    }
}
