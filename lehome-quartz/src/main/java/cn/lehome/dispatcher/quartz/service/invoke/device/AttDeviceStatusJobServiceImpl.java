package cn.lehome.dispatcher.quartz.service.invoke.device;

import cn.lehome.base.api.iot.common.bean.att.AttDevice;
import cn.lehome.base.api.iot.common.bean.att.QAttDevice;
import cn.lehome.base.api.iot.common.service.att.AttDeviceApiService;
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
@Service("attDeviceStatusJobService")
public class AttDeviceStatusJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private AttDeviceApiService attDeviceApiService;

    private static int ONLINE_INTERVAL = -2;

    @Override
    public void doInvoke(Map<String, String> params) {
        Date date = new Date();
        date = DateUtils.addMinutes(date, ONLINE_INTERVAL);
        ApiRequest apiRequest = ApiRequest.newInstance().filterLessThan(QAttDevice.lastOnlineTime, date);
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance().paging(0, 50);
        while (true) {
            ApiResponse<AttDevice> apiResponse = attDeviceApiService.findAll(apiRequest, apiRequestPage);

            if (apiResponse == null || apiResponse.getCount() == 0) {
                break;
            }

            List<AttDevice> attDeviceList = Lists.newArrayList(apiResponse.getPagedData());

            attDeviceList.forEach(attDevice -> attDevice.setOnlineStatus(OnlineStatus.OFFLINE));

            attDeviceApiService.batchUpdate(attDeviceList);

            if (apiResponse.getCount() < apiRequestPage.getPageSize()) {
                break;
            }

            apiRequestPage.pagingNext();
        }
    }
}
