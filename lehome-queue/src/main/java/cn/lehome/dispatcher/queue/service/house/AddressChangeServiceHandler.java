package cn.lehome.dispatcher.queue.service.house;

import cn.lehome.bean.pro.enums.address.ExtendType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by wuzhao on 2019/5/31.
 */
@Component
public class AddressChangeServiceHandler {

    @Autowired
    private AddressChangeService managerAreaAddressChangeService;

    @Autowired
    private AddressChangeService floorAddressChangeService;

    @Autowired
    private AddressChangeService unitAddressChangeService;

    @Autowired
    private AddressChangeService houseAddressChangeService;

    public AddressChangeService getAddressChangeService(ExtendType extendType) {
        switch (extendType) {
            case PROJECT :
                return managerAreaAddressChangeService;
            case BUILDING :
                return floorAddressChangeService;
            case UNIT :
                return unitAddressChangeService;
            default:
                return houseAddressChangeService;
        }
    }
}
