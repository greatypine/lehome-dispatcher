package cn.lehome.dispatcher.quartz.service.invoke;

import cn.lehome.base.api.common.bean.storage.ImageInfo;
import cn.lehome.base.api.common.bean.storage.QStorageInfo;
import cn.lehome.base.api.common.bean.storage.StorageInfo;
import cn.lehome.base.api.common.service.storage.StorageInfoApiService;
import cn.lehome.bean.common.enums.storage.SceneType;
import cn.lehome.bean.common.enums.storage.StorageIsCheckedType;
import cn.lehome.bean.common.enums.storage.StorageObjectType;
import cn.lehome.bean.common.enums.storage.StorageUsageType;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 检测用户上传图片定时任务
 * Created by zuoguodong on 2018/4/20
 */
@Service("checkUserUploadImageScheduleJobService")
public class CheckUserUploadImageScheduleJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private StorageInfoApiService storageInfoApiService;

//    @Autowired
//    private ImageSafeApiService imageSafeApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        logger.info("图片检测定时任务被调用");
        ApiRequest apiRequest = ApiRequest.newInstance();
        List<StorageObjectType> objectTypeList = Arrays.asList(StorageObjectType.USER,StorageObjectType.CONTENT);
        List<StorageUsageType> usageTypeList = Arrays.asList(StorageUsageType.USER_AVATAR,StorageUsageType.POST);
        apiRequest.filterIn(QStorageInfo.objectType,objectTypeList);
        apiRequest.filterIn(QStorageInfo.usageType,usageTypeList);
        apiRequest.filterEqual(QStorageInfo.isChecked, StorageIsCheckedType.UNCHECKED);
        ApiRequestPage requestPage = ApiRequestPage.newInstance();
        requestPage.paging(0,100);
        ApiResponse<StorageInfo> response =  storageInfoApiService.findAll(apiRequest,requestPage);
        if(response.getCount()==0){
            return;
        }
        List<ImageInfo> imageInfoList = response.getPagedData().stream().map(data->{
            ImageInfo imageInfo = new ImageInfo();
            imageInfo.setImageId(String.valueOf(data.getId()));
            imageInfo.setImageUrl(data.getPrefix()+data.getRelativeUrl());
            return imageInfo;
        }).collect(Collectors.toList());
        List<SceneType> sceneTypeList = Arrays.asList(SceneType.porn,SceneType.terrorism);
//        Map<String,String> result = imageSafeApiService.imageAsyncScan(imageInfoList,sceneTypeList);
//        storageInfoApiService.updateStorageInfoCheckedStatus(result);
    }
}
