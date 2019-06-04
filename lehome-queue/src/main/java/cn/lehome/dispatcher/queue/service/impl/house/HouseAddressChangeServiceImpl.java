package cn.lehome.dispatcher.queue.service.impl.house;

import cn.lehome.base.pro.api.bean.address.AddressBaseInfo;
import cn.lehome.base.pro.api.bean.house.AddressBean;
import cn.lehome.base.pro.api.bean.house.HouseInfo;
import cn.lehome.base.pro.api.service.address.AddressBaseApiService;
import cn.lehome.base.pro.api.service.house.HouseInfoApiService;
import cn.lehome.bean.pro.enums.address.ExtendType;
import cn.lehome.dispatcher.queue.service.house.AddressChangeService;
import cn.lehome.dispatcher.queue.service.impl.AbstractBaseServiceImpl;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by wuzhao on 2019/5/31.
 */
@Service("houseAddressChangeService")
public class HouseAddressChangeServiceImpl extends AbstractBaseServiceImpl implements AddressChangeService {

    @Autowired
    private AddressBaseApiService addressBaseApiService;

    @Autowired
    private HouseInfoApiService houseInfoApiService;

    @Override
    public void changeName(Integer id) {
        HouseInfo houseInfo = houseInfoApiService.findOne(id.longValue());
        if (houseInfo == null) {
            logger.error("房间未找到, id = " + id);
            return;
        }
        // TODO: 2019/5/31 修改住户信息
        AddressBaseInfo addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.HOUSE, id.longValue());
        if (addressBaseInfo == null) {
            logger.error("房间地址未找到, id = " + id);
            return;
        }
        AddressBean addressBean = JSON.parseObject(addressBaseInfo.getAddress(), AddressBean.class);
        addressBean.setRoomName(houseInfo.getRoomName());
        addressBean.setRoomId(houseInfo.getRoomId());
        addressBaseInfo.setAddress(JSON.toJSONString(addressBean));
        addressBaseApiService.update(addressBaseInfo);
    }
}
