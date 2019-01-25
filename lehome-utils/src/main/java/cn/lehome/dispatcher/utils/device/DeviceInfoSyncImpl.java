package cn.lehome.dispatcher.utils.device;

import cn.lehome.base.api.oauth2.bean.device.MobileDevice;
import cn.lehome.base.api.oauth2.bean.device.QClientDevice;
import cn.lehome.base.api.oauth2.service.device.ClientDeviceApiService;
import cn.lehome.base.api.tool.bean.device.ClientDevice;
import cn.lehome.base.api.tool.bean.device.PushDeviceInfo;
import cn.lehome.base.api.tool.service.device.DeviceApiService;
import cn.lehome.bean.tool.entity.enums.device.PushVendorType;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.bean.core.enums.ClientOSType;
import cn.lehome.framework.bean.core.enums.DeviceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by wuzhao on 2018/12/4.
 */
@Service("deviceInfoSync")
public class DeviceInfoSyncImpl implements DeviceInfoSync {

    private static Logger logger = LoggerFactory.getLogger(DeviceInfoSync.class);

    @Autowired
    private DeviceApiService deviceApiService;

    @Autowired
    private ClientDeviceApiService clientDeviceApiService;

    @Override
    public void sync(Long startId) {
        ApiRequestPage requestPage = ApiRequestPage.newInstance().paging(0, 100).addOrder(QClientDevice.id, PageOrderType.ASC);
        ApiRequest apiRequest = ApiRequest.newInstance();
        if (startId != null && startId > 0L) {
            apiRequest.filterGreaterEqual(QClientDevice.id, startId);
        }
        int i = 0;
        while (true) {
            ApiResponse<ClientDevice> apiResponse = deviceApiService.findAll(apiRequest, requestPage);

            if (apiResponse == null || apiResponse.getCount() == 0) {
                break;
            }

            for (ClientDevice clientDevice : apiResponse.getPagedData()) {
                try {
                    syncDevice(clientDevice);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            i += apiResponse.getPagedData().size();
            System.out.println("同步完成" + i + "条数据");


            if (apiResponse.getCount() < requestPage.getPageSize()) {
                break;
            }

            requestPage.pagingNext();
        }
        System.out.println("全部同步完成, 总共 ： " + i);
    }

    private void syncDevice(ClientDevice clientDevice) {
        DeviceType deviceType = DeviceType.IPHONE;
        if (clientDevice.getClientOSType().equals(ClientOSType.ANDROID)) {
            deviceType = DeviceType.ANDROID;
        }
        cn.lehome.base.api.oauth2.bean.device.ClientDevice oauth2ClientDevice = clientDeviceApiService.getWithCreateMobileDevice("sqbj-server", clientDevice.getClientId(), deviceType);

        MobileDevice mobileDevice = new MobileDevice();
        mobileDevice.setDeviceUid(clientDevice.getClientId());
        mobileDevice.setClientId("sqbj-server");
        mobileDevice.setClientDeviceId(oauth2ClientDevice.getId());
        mobileDevice.setOsName(clientDevice.getClientOsName());
        mobileDevice.setType(deviceType);
        mobileDevice.setOsVersion(clientDevice.getClientOsVersion());
        mobileDevice.setDeviceModule(clientDevice.getClientModule());
        mobileDevice.setAppVersion(clientDevice.getAppVersion());
        mobileDevice.setAppVersionCode(clientDevice.getAppVersionCode());
        mobileDevice.setChannel(clientDevice.getChannel());
        mobileDevice.setCreatedTime(clientDevice.getCreatedTime());
        mobileDevice.setUpdatedTime(clientDevice.getUpdatedTime());
        PushDeviceInfo pushDeviceInfo = deviceApiService.getPushDeviceInfo(clientDevice.getId(), PushVendorType.JIGUANG);
        if (pushDeviceInfo != null) {
            mobileDevice.setVendorType(cn.lehome.framework.bean.core.enums.PushVendorType.JIGUANG);
            mobileDevice.setVendorClientId(pushDeviceInfo.getVendorClientId());
        } else {
            mobileDevice.setVendorType(cn.lehome.framework.bean.core.enums.PushVendorType.NONE);
        }
        clientDeviceApiService.uploadClientDevice(mobileDevice);
    }
}
