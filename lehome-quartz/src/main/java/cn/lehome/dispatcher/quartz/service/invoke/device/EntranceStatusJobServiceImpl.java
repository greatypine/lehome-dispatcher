package cn.lehome.dispatcher.quartz.service.invoke.device;

import cn.lehome.base.api.iot.common.bean.entrance.EntranceDevice;
import cn.lehome.base.api.iot.common.bean.entrance.QEntranceDevice;
import cn.lehome.base.api.iot.common.service.entrance.EntranceDeviceApiService;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.iot.bean.common.enums.gateway.OnlineStatus;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by wuzhao on 2019/10/11.
 */
@Service("entranceStatusJobService")
public class EntranceStatusJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private EntranceDeviceApiService entranceDeviceApiService;

    private static int ONLINE_INTERVAL = -2;

    @Override
    public void doInvoke(Map<String, String> params) {
        Date date = new Date();
        date = DateUtils.addMinutes(date, ONLINE_INTERVAL);
        ApiRequest apiRequest = ApiRequest.newInstance().filterLessThan(QEntranceDevice.lastOnlineTime, date);
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance().paging(0, 50);
        while (true) {
            ApiResponse<EntranceDevice> apiResponse = entranceDeviceApiService.findAll(apiRequest, apiRequestPage);

            if (apiResponse == null || apiResponse.getCount() == 0) {
                break;
            }

            List<EntranceDevice> entranceDeviceList = Lists.newArrayList(apiResponse.getPagedData());

            entranceDeviceList.forEach(entranceDevice -> entranceDevice.setOnlineStatus(OnlineStatus.OFFLINE));

            entranceDeviceApiService.batchSave(entranceDeviceList);

            if (apiResponse.getCount() < apiRequestPage.getPageSize()) {
                break;
            }

            apiRequestPage.pagingNext();
        }
    }
}
