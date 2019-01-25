package cn.lehome.dispatcher.utils.community;

import cn.lehome.base.api.content.bean.post.PostInfo;
import cn.lehome.base.api.content.bean.post.QPostInfo;
import cn.lehome.base.api.content.service.post.PostInfoApiService;
import cn.lehome.base.api.property.bean.area.AreaInfoDetail;
import cn.lehome.base.api.property.service.area.AreaInfoApiService;
import cn.lehome.base.api.tool.bean.community.Community;
import cn.lehome.base.api.tool.bean.community.CommunityExt;
import cn.lehome.base.api.tool.bean.community.QCommunityExt;
import cn.lehome.base.api.tool.compoment.jms.EventBusComponent;
import cn.lehome.base.api.tool.constant.EventConstants;
import cn.lehome.base.api.tool.service.community.CommunityApiService;
import cn.lehome.base.api.tool.service.community.CommunityCacheApiService;
import cn.lehome.bean.content.entity.enums.post.UserType;
import cn.lehome.bean.tool.entity.enums.community.EditionType;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.ExcelUtils;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by zuoguodong on 2018/3/20
 */
@Service
public class CommunityServiceImpl implements CommunityService {

    @Autowired
    private CommunityApiService communityApiService;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Autowired
    private PostInfoApiService postInfoApiService;

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    @Autowired
    private AreaInfoApiService areaInfoApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Override
    public void bindExt(String[] args) {
        if(args.length<2){
            System.out.println("excel文件路径不能为空");
            return;
        }
        String file = args[1];
        int sheetIndex = 0;
        if(args.length>2){
            try {
                sheetIndex = Integer.valueOf(args[2]);
            }catch(Exception e){
                System.out.println("sheetIndex 必须是数字");
                return;
            }
        }
        try {
            ExcelUtils excelUtils = new ExcelUtils(file);
            List<List<String>> data = excelUtils.read(sheetIndex);
            for (List<String> rowList : data) {
                if (rowList.size() < 4) {
                    continue;
                }
                Long communityExtId;
                try {
                    communityExtId = Long.valueOf(rowList.get(0));
                }catch(Exception e){
                    System.out.println("id 必须是数字 ：" + rowList.get(0));
                    continue;
                }
                if(null == rowList.get(3) || "null".equals(rowList.get(3))){
                    continue;
                }
                String[] communityIds = rowList.get(3).split(",");
                List<String> communityIdList = new ArrayList<>();
                for (String communityId : communityIds) {
                    communityIdList.add(communityId);
                }
                try {
                    communityApiService.bindExt(communityExtId, communityIdList);
                }catch(Exception e){
                    System.out.println(communityExtId + ":" + e.getMessage());
                }
            }
        }catch(Exception e){
            System.out.println("数据处理出错：" + e.getMessage());
            e.printStackTrace();
            return;
        }
        System.out.println("数据处理完毕");
    }

    @Override
    public void updateCommunityExtName(String[] args) {
        if(args.length < 3){
            System.out.println("参数错误");
            return;
        }
        Long communityId = 0l;
        try{
           communityId = Long.valueOf(args[1]);
        }catch(Exception e){
            System.out.println("参数错误");
            return;
        }
        String communityName = args[2];
        CommunityExt communityExt = communityApiService.getExt(communityId);
        if(communityExt==null){
            System.out.println("小区未找到");
            return;
        }
        communityExt.setName(communityName);
        communityApiService.saveExt(communityExt);
        System.out.println("数据处理完毕");
    }


    @Override
    public void initPost(String input[]) {
        if(input.length>1){
            for(int i = 1;i<input.length;i++){
                Long communityId = Long.valueOf(input[i]);
                try {
                    this.sendInitCommunityPostMessage(communityId);
                }catch(Exception e){
                    System.out.println("发送数据时出错：" + communityId);
                    e.printStackTrace();
                }
            }
        }else{
            List<Community> communityList = communityApiService.findAll(ApiRequest.newInstance());
            communityList.forEach(community -> {
                try {
                    this.sendInitCommunityPostMessage(community.getId());
                }catch(Exception e){
                    System.out.println("发送数据时出错：" + community.getId());
                    e.printStackTrace();
                }
            });
        }
        System.out.println("数据处理完毕");
    }

    @Override
    public void cleanPost() {
        ApiRequest apiRequest = ApiRequest.newInstance();
        apiRequest.filterEqual(QPostInfo.userType, UserType.ROBOT);
        int pageNo = 0;
        int pageSize = 100;
        int count = 0;
        ApiResponse<PostInfo> apiResponse = postInfoApiService.findAll(apiRequest, ApiRequestPage.newInstance().paging(pageNo,pageSize));
        while(apiResponse.getCount() > 0){
            count += apiResponse.getCount();
            final Collection<PostInfo> collection = apiResponse.getPagedData();
            threadPoolExecutor.execute(() ->
                collection.forEach(postInfo -> postInfoApiService.delete(postInfo.getPostId(), YesNoStatus.NO))
            );
            pageNo ++;
            System.out.println("pageNo:" + pageNo);
            apiResponse = postInfoApiService.findAll(apiRequest, ApiRequestPage.newInstance().paging(pageNo,pageSize));
        }
        while (threadPoolExecutor.getActiveCount() != 0) {
            try {
                System.out.println("cleanPost 数据加载完毕" + count + "，还有" + threadPoolExecutor.getQueue().size() + "任务等执行");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("任务执行完毕");
    }

    @Override
    public void updateCommunityUniqueCode(String accessToken) {
        Map<String, String> headerMap = Maps.newHashMap();
        headerMap.put("API-PRO-APP-OAUTH-TOKEN", accessToken);
        headerMap.put("Edition-Type", "pro");
        headerMap.put("API-APP-ID", "pro-ios");
        headerMap.put("API-Client-ID", "xxxx");
        List<CommunityExt> communityExtList = communityApiService.findAllExt(ApiRequest.newInstance().filterEqual(QCommunityExt.editionType, EditionType.pro));
        for (CommunityExt communityExt : communityExtList) {
            System.out.println("认证小区ID:" + communityExt.getId() + ", 物管小区ID : " + communityExt.getPropertyCommunityId());
            try {
                AreaInfoDetail areaInfoDetail = areaInfoApiService.findOne(communityExt.getPropertyCommunityId(), headerMap);
                if (StringUtils.isNotEmpty(areaInfoDetail.getUniqueCode())) {
                    System.out.println("uniqueCode :" + areaInfoDetail.getUniqueCode());
                    communityExt.setUniqueCode(areaInfoDetail.getUniqueCode());
                    communityApiService.updateExt(communityExt);
                }
            } catch (Exception e) {
                System.out.println("认证小区ID : " + communityExt.getId() + "修正失败");
            }

        }
    }

    private void sendInitCommunityPostMessage(Long communityId){
        eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.THIRD_COMMUNITY_EVENT, communityId));
    }


    @Override
    public void updateAllCommunityExt() {
        List<CommunityExt> communityExtList = communityApiService.findAllExt(ApiRequest.newInstance());
        for (CommunityExt communityExt : communityExtList) {
            try {
                System.out.println("开始更新ext数据到缓存");
                communityApiService.updateExt(communityExt);
            } catch (Exception e) {
                System.out.println("认证小区ID : " + communityExt.getId() + "修正失败");
            }
        }
        System.out.println("刷新communityExt数据完毕共:"+communityExtList.size());
    }

    @Override
    public void updateAllCommunity() {
        ApiRequest apiRequest = ApiRequest.newInstance();
        int pageNo = 0;
        int pageSize = 100;
        int count = 0;
        ApiResponse<Community> communityApiResponse = communityApiService.findAll(apiRequest,ApiRequestPage.newInstance().paging(pageNo,pageSize));
        while (communityApiResponse.getPagedData().size()>0){
            count += communityApiResponse.getPagedData().size();
            List<Community> communities = Lists.newArrayList(communityApiResponse.getPagedData());
            for (Community community : communities){
                communityCacheApiService.saveOrUpdateCommunity(community);
            }
            pageNo ++;
            communityApiResponse = communityApiService.findAll(apiRequest,ApiRequestPage.newInstance().paging(pageNo,pageSize));
        }
        System.out.println("刷新community数据完毕共:"+count);
    }



}
