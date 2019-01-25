package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.tool.bean.green.ImageScanTask;
import cn.lehome.base.api.tool.service.green.ImageSafeApiService;
import cn.lehome.base.api.tool.service.storage.AliyunImageScanTaskApiService;
import cn.lehome.bean.tool.entity.enums.storage.ImageScanTaskStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by zuoguodong on 2018/4/24
 */
@Service("getUploadImageCheckResultScheduleJobService")
public class GetUploadImageCheckResultScheduleJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private ImageSafeApiService imageSafeApiService;

    @Autowired
    private AliyunImageScanTaskApiService aliyunImageScanTaskApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        ApiRequest apiRequest = ApiRequest.newInstance();
        apiRequest.filterEqual("taskStatus", ImageScanTaskStatus.UNFINISHED);
        ApiRequestPage requestPage = ApiRequestPage.newInstance();
        requestPage.paging(0,100);
        ApiResponse<ImageScanTask> response = aliyunImageScanTaskApiService.findAll(apiRequest,requestPage);
        if(response.getCount()==0){
            return;
        }
        List<String> taskList = response.getPagedData().stream().map(m -> m.getTaskId()).collect(Collectors.toList());
        List<ImageScanTask> imageScanResultList = imageSafeApiService.getImageAsyncScanResults(taskList);
        aliyunImageScanTaskApiService.updateImageScanTaskStatus(imageScanResultList);
    }

}
