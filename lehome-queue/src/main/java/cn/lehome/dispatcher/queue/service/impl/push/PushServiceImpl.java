package cn.lehome.dispatcher.queue.service.impl.push;

import cn.lehome.base.api.common.bean.community.Community;
import cn.lehome.base.api.common.bean.community.QCommunity;
import cn.lehome.base.api.common.bean.device.ClientDeviceIndex;
import cn.lehome.base.api.common.bean.device.QClientDevice;
import cn.lehome.base.api.common.bean.region.RegionInfo;
import cn.lehome.base.api.common.operation.bean.push.PushPlan;
import cn.lehome.base.api.common.service.community.CommunityApiService;
import cn.lehome.base.api.common.service.device.ClientDeviceIndexApiService;
import cn.lehome.base.api.common.service.region.RegionInfoApiService;
import cn.lehome.base.api.user.bean.user.QUserInfoIndex;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.common.enums.region.RegionType;
import cn.lehome.bean.user.entity.enums.user.UserAuthType;
import cn.lehome.bean.user.entity.enums.user.UserStatus;
import cn.lehome.dispatcher.queue.service.impl.AbstractBaseServiceImpl;
import cn.lehome.dispatcher.queue.service.push.PushService;
import cn.lehome.framework.base.api.core.compoment.request.ApiPageRequestHelper;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.ExcelUtils;
import cn.lehome.framework.bean.core.enums.*;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
@Service
public class PushServiceImpl extends AbstractBaseServiceImpl implements PushService{


    @Autowired
    private CommunityApiService communityApiService;

    @Autowired
    private RegionInfoApiService regionInfoApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private ClientDeviceIndexApiService clientDeviceIndexApiService;

    private static final Long country = 100000L;

    private static final Integer ES_MAX_COUNT = 1024;


    public Map<String,List<UserInfoIndex>> findAllUserInfo(PushPlan pushPlan){
        Map<String ,List<UserInfoIndex>> userInfoIndexMap = Maps.newHashMap();
        if ((!pushPlan.getPushType().equals(PushType.PUSH))){
            logger.error("进入站内信或短信推送查询方法");
            userInfoIndexMap.put("commonUserInfos",this.findAllUserData(pushPlan));
        }else{
            if (pushPlan.getPushTemplateProperty() !=null && pushPlan.getPushTemplateProperty().getIsContainsStationLetter().equals(YesNoStatus.YES)){
                logger.error("进入push包含站内信查询方法");
                userInfoIndexMap.put("pushUserInfos",this.findAllUserData(pushPlan));
                pushPlan.setPushType(PushType.STATION_LETTER);
                userInfoIndexMap.put("commonUserInfos",this.findAllUserData(pushPlan));
                pushPlan.setPushType(PushType.PUSH);
            }else{
                logger.error("进入单纯push查询方法");
                userInfoIndexMap.put("pushUserInfos",this.findAllUserData(pushPlan));
            }
        }
        return userInfoIndexMap;
    }


    private Set<Long> findAllCommunity(PushPlan pushPlan){
        Set<Long> communityIds = new HashSet<>();
        List<Long> community = JSON.parseArray(pushPlan.getCommunityIds(),Long.class);
        List<Long> region = JSON.parseArray(pushPlan.getRegionIds(),Long.class);
        if (CollectionUtils.isNotEmpty(community)){
            communityIds.addAll(community);
        }
        if (CollectionUtils.isNotEmpty(region)){
            if (region.contains(country)){
                List<Community> all = ApiPageRequestHelper.request(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(0, 100), communityApiService::findAll);
                List<Long> ids = all.stream().map(Community::getId).collect(Collectors.toList());
                communityIds.addAll(ids);
            }else{
                Map<Long, RegionInfo> regionInfoMap = regionInfoApiService.findAll(region);
                List<Long> pCode = regionInfoMap.values().stream().filter(regionInfo -> regionInfo.getType().equals(RegionType.PROVINCE)).map(RegionInfo::getId).collect(Collectors.toList());
                List<String> cityCode = regionInfoMap.values().stream().filter(regionInfo -> regionInfo.getType().equals(RegionType.CITY)).map(RegionInfo::getCityCode).collect(Collectors.toList());
                List<Long> adCode = regionInfoMap.values().stream().filter(regionInfo -> regionInfo.getType().equals(RegionType.DISTRICT)).map(RegionInfo::getId).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(pCode)){
                    List<Community> communityList = ApiPageRequestHelper.request(ApiRequest.newInstance().filterIn(QCommunity.pcode, pCode), ApiRequestPage.newInstance().paging(0, 100), communityApiService::findAll);
                    if(CollectionUtils.isNotEmpty(communityList)){
                        List<Long> ids = communityList.stream().map(Community::getId).collect(Collectors.toList());
                        communityIds.addAll(ids);
                    }
                }
                if (CollectionUtils.isNotEmpty(cityCode)){
                    List<Community> communityList = ApiPageRequestHelper.request(ApiRequest.newInstance().filterIn(QCommunity.citycode, cityCode), ApiRequestPage.newInstance().paging(0, 100), communityApiService::findAll);
                    if (CollectionUtils.isNotEmpty(communityList)){
                        List<Long> ids = communityList.stream().map(Community::getId).collect(Collectors.toList());
                        communityIds.addAll(ids);
                    }
                }
                if (CollectionUtils.isNotEmpty(adCode)){
                    List<Community> communityList = ApiPageRequestHelper.request(ApiRequest.newInstance().filterIn(QCommunity.adcode, adCode), ApiRequestPage.newInstance().paging(0, 100), communityApiService::findAll);
                    if (CollectionUtils.isNotEmpty(communityList)){
                        List<Long> ids = communityList.stream().map(Community::getId).collect(Collectors.toList());
                        communityIds.addAll(ids);
                    }
                }
            }
        }

        return communityIds;
    }

    @Override
    public List<Long> readExcelUserInfo(String ossUrl) {
        String pathName = this.downloadFileFromOss(ossUrl);
        List<Long> userPhoneList = Lists.newArrayList();
        ExcelUtils excelUtils = new ExcelUtils(pathName);
        List<List<String>> data = excelUtils.read(0);
        for (List<String> rows : data){
            if (rows == null || rows.get(0) == null || StringUtils.isBlank(rows.get(0))) {
                continue;
            }
            if (rows.size()<1){
                continue;
            }
            Long userPhone = Long.parseLong(rows.get(0).trim());
            userPhoneList.add(userPhone);
        }
        //删除下载的excel文件
        this.deleteFile(pathName);
        List<UserInfoIndex> userInfoIndexList = Lists.newArrayList();
        if (userPhoneList.size()>ES_MAX_COUNT){
            List<List<Long>> subUserInfos = Lists.partition(userPhoneList,ES_MAX_COUNT);
            for (List<Long> userInfos : subUserInfos){
                List<UserInfoIndex> userInfoIndices = userInfoIndexApiService.findAll(ApiRequest.newInstance().filterIn(QUserInfoIndex.phone,userInfos));
                userInfoIndexList.addAll(userInfoIndices);
            }
        }else {
            userInfoIndexList = userInfoIndexApiService.findAll(ApiRequest.newInstance().filterIn(QUserInfoIndex.phone,userPhoneList));
        }
        List<Long> userIdList = userInfoIndexList.stream().map(UserInfoIndex::getId).collect(Collectors.toList());
        return userIdList;
    }

    @Override
    public Set<String> readExcelUserInfoMobiles(String ossUrl) {

        String pathName = this.downloadFileFromOss(ossUrl);
        Set<String> userPhoneSet = Sets.newHashSet();
        ExcelUtils excelUtils = new ExcelUtils(pathName);
        List<List<String>> data = excelUtils.read(0);
        for (List<String> rows : data){
            if (rows.size()<1){
                continue;
            }
            if(StringUtils.isNotEmpty(rows.get(0))){
                userPhoneSet.add(rows.get(0).trim());
            }
        }
        //删除下载的excel文件
        this.deleteFile(pathName);
        return userPhoneSet;
    }

    private Set<UserInfoIndex> queryAllUsersData(Set<Long> communitySet, ApiRequest apiRequest,
                                                 ApiRequestPage apiRequestPage, PushPlan pushPlan) {
        Set<UserInfoIndex> userInfoIndexAll = Sets.newHashSet();
        //认证的小区id记的都是communityExtId
        List<Long> communityListAll = Lists.newArrayList(communitySet);
        if (communityListAll.size() > ES_MAX_COUNT) {
            List<List<Long>> communityListPartition = Lists.partition(communityListAll, ES_MAX_COUNT);
            for (List<Long> subCommunityList : communityListPartition) {
                this.addAllUserInfos(userInfoIndexAll, subCommunityList, apiRequest, apiRequestPage, pushPlan);
            }
        } else {
            this.addAllUserInfos(userInfoIndexAll, communityListAll, apiRequest, apiRequestPage, pushPlan);
        }
        return userInfoIndexAll;
    }

    /**
     * 根据pushplan查询所有用户信息
     * @param pushPlan
     * @return userInfoIndexSetAll
     */
    public List<UserInfoIndex> findAllUserData(PushPlan pushPlan){
        Set<Long> communitySet = this.findAllCommunity(pushPlan);
        logger.error("communitySet 集合为:"+ communitySet.size());
        Set<UserInfoIndex> userInfoIndexSetAll = Sets.newHashSet();
        if (CollectionUtils.isNotEmpty(communitySet)){
            int pageSize = 1000;
            ApiRequest apiRequest = ApiRequest.newInstance().filterLike(QUserInfoIndex.del, UserStatus.NotDeleted);
            ApiRequestPage apiRequestPage = ApiRequestPage.newInstance().paging(0, pageSize).addOrder(QUserInfoIndex.id, PageOrderType.ASC);
            if(PushUserType.AUTH.equals(pushPlan.getPushUserType())) {
                //认证的小区id记的都是communityExtId
                Set<UserInfoIndex> userInfoIndexSet = this.queryAuthUserData(apiRequest,apiRequestPage,communitySet,pushPlan);
                userInfoIndexSetAll.addAll(userInfoIndexSet);
            } else if (PushUserType.NOT_AUTH.equals(pushPlan.getPushUserType())) {
                Set<UserInfoIndex> userInfoIndexSet = this.queryNotAuthUserData(communitySet,apiRequest,apiRequestPage, pushPlan);
                userInfoIndexSetAll.addAll(userInfoIndexSet);
            } else {
                logger.error("进入全部查询全部类型方法");
                Set<UserInfoIndex> userInfoAll = this.queryAllUsersData(communitySet,apiRequest,apiRequestPage, pushPlan);
                userInfoIndexSetAll.addAll(userInfoAll);
            }
        }
        return Lists.newArrayList(userInfoIndexSetAll);
    }

    private void addAllUserInfos(Set<UserInfoIndex> userInfoIndexAll, List<Long> subCommunityList, ApiRequest apiRequest, ApiRequestPage apiRequestPage, PushPlan pushPlan) {
        //先查认证用户手机号
        //认证的小区id记的都是communityExtId
        Set<UserInfoIndex> authUserInfos = Sets.newHashSet();
        Set<Long> communityExtSet = this.findCommunityExtSet(subCommunityList);
        logger.error("communityExtSet: "+ communityExtSet.size());
        if (CollectionUtils.isNotEmpty(communityExtSet)){
            apiRequest.filterIn(QUserInfoIndex.authCommunityIds, communityExtSet);
            authUserInfos = this.getUserIndexInfos(apiRequest, apiRequestPage, pushPlan);
            logger.error("authUserInfos: "+ authUserInfos.size());
        }
        //用新的查询条件再查一次非认证的手机号
        ApiRequest apiRequestNew = ApiRequest.newInstance().filterLike(QUserInfoIndex.del, UserStatus.NotDeleted);
        ApiRequestPage apiRequestPageNew = ApiRequestPage.newInstance().paging(0, 1000).addOrder(QUserInfoIndex.id, PageOrderType.ASC);
        apiRequestNew.filterIn(QUserInfoIndex.bindCommunityIds, subCommunityList);
        if (CollectionUtils.isNotEmpty(communityExtSet)){
            apiRequestNew.filterNotIn(QUserInfoIndex.authCommunityIds, communityExtSet);
        }
        Set<UserInfoIndex> notAuthUserInfos = this.getUserIndexInfos(apiRequestNew, apiRequestPageNew, pushPlan);
        logger.error("notAuthUserInfos: "+ notAuthUserInfos.size());
        //合并认证用户和非认证用户的手机号
        userInfoIndexAll.addAll(authUserInfos);
        userInfoIndexAll.addAll(notAuthUserInfos);
    }


    private Set<UserInfoIndex> queryNotAuthUserData(Set<Long> communitySet, ApiRequest apiRequest, ApiRequestPage apiRequestPage, PushPlan pushPlan) {
        Set<UserInfoIndex> userInfoIndexAll = Sets.newHashSet();
        //认证的小区id记的都是communityExtId
        List<Long> communityListAll = Lists.newArrayList(communitySet);
        communityListAll.addAll(communitySet);
        if (communityListAll.size() > ES_MAX_COUNT) {
            List<List<Long>> communityListPartition = Lists.partition(communityListAll, ES_MAX_COUNT);
            for (List<Long> subCommunityList : communityListPartition) {
                this.addNotAuthUserInfos(userInfoIndexAll, subCommunityList, apiRequest, apiRequestPage, pushPlan);
            }
        } else {
            this.addNotAuthUserInfos(userInfoIndexAll, communityListAll, apiRequest, apiRequestPage, pushPlan);
        }
        return userInfoIndexAll;
    }


    private void addNotAuthUserInfos(Set<UserInfoIndex> userInfoIndexSet, List<Long> communityList, ApiRequest apiRequest,
                                     ApiRequestPage apiRequestPage, PushPlan pushPlan) {
        //对应的认证小区
        Set<Long> communityExtSet = this.findCommunityExtSet(communityList);
        if (CollectionUtils.isNotEmpty(communityExtSet)){
            apiRequest.filterNotIn(QUserInfoIndex.authCommunityIds, communityExtSet);
        }
        //在绑定的小区里,不在认证的小区里,就是非认证用户
        apiRequest.filterIn(QUserInfoIndex.bindCommunityIds, communityList);
        Set<UserInfoIndex> notAuthUserInfos = this.getUserIndexInfos(apiRequest, apiRequestPage, pushPlan);
        userInfoIndexSet.addAll(notAuthUserInfos);
    }

    private Set<UserInfoIndex> queryAuthUserData(ApiRequest apiRequest,ApiRequestPage apiRequestPage, Set<Long> communitySet,PushPlan pushPlan) {
        Set<UserInfoIndex> userInfoIndexAll = Sets.newHashSet();
        List<Long> communityListAll = Lists.newArrayList(communitySet);
        if (CollectionUtils.isNotEmpty(communityListAll)){
            apiRequest.filterLike(QUserInfoIndex.userAuthType, UserAuthType.AUTH);
            if (communityListAll.size()>ES_MAX_COUNT){
                List<List<Long>> subCommunityList = Lists.partition(communityListAll,ES_MAX_COUNT);
                for (List<Long> communitys :subCommunityList){
                    addAuthUserInfoIndex(userInfoIndexAll,communitys,apiRequest,apiRequestPage,pushPlan);
                }
            }else {
                addAuthUserInfoIndex(userInfoIndexAll,communityListAll,apiRequest,apiRequestPage,pushPlan);
            }
        }
        return userInfoIndexAll;
    }


    private void addAuthUserInfoIndex(Set<UserInfoIndex> userInfoIndices, List<Long> communityList, ApiRequest apiRequest,
                                      ApiRequestPage apiRequestPage, PushPlan pushPlan) {
        Set<Long> communityExtSet = this.findCommunityExtSet(communityList);
        if (CollectionUtils.isNotEmpty(communityExtSet)){
            apiRequest.filterIn(QUserInfoIndex.authCommunityIds, communityExtSet);
            Set<UserInfoIndex> userInfoIndexSet =getUserIndexInfos(apiRequest,apiRequestPage,pushPlan);
            userInfoIndices.addAll(userInfoIndexSet);
        }
    }



    private Set<UserInfoIndex> getUserIndexInfos(ApiRequest apiRequest, ApiRequestPage apiRequestPage, PushPlan pushPlan) {
        boolean flag = true;
        Long maxUserId = 0L;
        Set<UserInfoIndex> userInfoSet = Sets.newHashSet();
        while(flag){
            apiRequest.filterGreaterThan(QUserInfoIndex.id, maxUserId);
            ApiResponse<UserInfoIndex> apiResponse = userInfoIndexApiService.findAll(apiRequest, apiRequestPage);
            ArrayList<UserInfoIndex> userInfoIndexList = Lists.newArrayList(apiResponse.getPagedData());
            if (CollectionUtils.isNotEmpty(userInfoIndexList)){
                if (pushPlan.getPushType().equals(PushType.PUSH)){
                    addAllUserInfoSet(userInfoIndexList,pushPlan,userInfoSet);
                    logger.error("PUSH userInfoSet "+userInfoSet.size());
                }else {
                    if (pushPlan.getPushOsType().equals(PushOsType.ALL) && pushPlan.getAppVersionCode()==0L){
                        userInfoSet.addAll(userInfoIndexList);
                        logger.error("ALL userInfoSet "+userInfoSet.size());
                    }else {
                        addAllUserInfoSet(userInfoIndexList,pushPlan,userInfoSet);
                        logger.error(pushPlan.getPushOsType()+"TYPE userInfoSet "+userInfoSet.size());
                    }

                }
                maxUserId = userInfoIndexList.get(userInfoIndexList.size() - 1).getId();
            }else {
                flag = false;
            }
        }
        return userInfoSet;
    }



    private void addAllUserInfoSet(List<UserInfoIndex> userInfoIndexList, PushPlan pushPlan, Set<UserInfoIndex> userInfoSet){
        Set<String> clientIdList = Sets.newHashSet();
        List<ClientDeviceIndex> clientDeviceIndices = Lists.newArrayList();
        userInfoIndexList.forEach(userInfoIndex -> {
            if (StringUtils.isNotEmpty(userInfoIndex.getClientId())) {
                clientIdList.add(userInfoIndex.getClientId());
            } else {
                if (StringUtils.isNotEmpty(userInfoIndex.getLastClientId())) {
                    clientIdList.add(userInfoIndex.getLastClientId());
                }
            }
        });
        if (CollectionUtils.isNotEmpty(clientIdList)){
            ApiRequest apiRequest =  ApiRequest.newInstance().filterIn(QClientDevice.clientId, clientIdList);
            if (pushPlan.getAppVersionCode() == 0L){
                if (pushPlan.getPushOsType().equals(PushOsType.ALL)){
                    List<String> pushOsTypes = Arrays.asList(PushOsType.ANDROID.toString(),PushOsType.IOS.toString());
                    apiRequest.filterLikes(QClientDevice.clientOSType,pushOsTypes);
                }else{
                    apiRequest.filterLike(QClientDevice.clientOSType,pushPlan.getPushOsType());
                }
                clientDeviceIndices = clientDeviceIndexApiService.findAll(apiRequest);
            }else {
                logger.info("进入按照APP版本筛选用户方法");
                if (pushPlan.getPushOsType().equals(PushOsType.ALL)){
                    ApiRequest apiRequestAndroid =  ApiRequest.newInstance().filterIn(QClientDevice.clientId, clientIdList).filterGreaterEqual(QClientDevice.appVersionCode,pushPlan.getAppVersionCode()).filterLike(QClientDevice.clientOSType, ClientOSType.ANDROID);
                    List<ClientDeviceIndex> clientDeviceIndicesAndroid = clientDeviceIndexApiService.findAll(apiRequestAndroid);
                    ApiRequest apiRequestIOS = ApiRequest.newInstance().filterIn(QClientDevice.clientId, clientIdList).filterGreaterEqual(QClientDevice.appVersionCode,pushPlan.getAppVersionCode()).filterLike(QClientDevice.clientOSType, ClientOSType.IOS);
                    List<ClientDeviceIndex> clientDeviceIndicesIOS = clientDeviceIndexApiService.findAll(apiRequestIOS);
                    clientDeviceIndices.addAll(clientDeviceIndicesAndroid);
                    clientDeviceIndices.addAll(clientDeviceIndicesIOS);
                }else {
                    apiRequest.filterLike(QClientDevice.clientOSType, pushPlan.getPushOsType());
                    apiRequest.filterGreaterEqual(QClientDevice.appVersionCode,pushPlan.getAppVersionCode());
                    clientDeviceIndices = clientDeviceIndexApiService.findAll(apiRequest);
                }
            }
            Set<String> afterFilterClientSet = clientDeviceIndices.stream().map(ClientDeviceIndex::getClientId).collect(Collectors.toSet());
            List<UserInfoIndex> afterUserInfoIndexList = Lists.newArrayList();
            if (CollectionUtils.isNotEmpty(afterFilterClientSet)) {
                for (UserInfoIndex userInfoIndex : userInfoIndexList) {
                    if ((StringUtils.isNotEmpty(userInfoIndex.getClientId()) && afterFilterClientSet.contains(userInfoIndex.getClientId()))
                            || (StringUtils.isNotEmpty(userInfoIndex.getLastClientId()) && afterFilterClientSet.contains(userInfoIndex.getLastClientId()))) {
                        afterUserInfoIndexList.add(userInfoIndex);
                    }
                }
                if (CollectionUtils.isNotEmpty(afterUserInfoIndexList)){
                    userInfoSet.addAll(afterUserInfoIndexList);
                }
            }
        }
    }

    /**
     * 下载oss上的文件,返回本地地址和文件名
     * @param ossUrl
     * @return
     */
    private String downloadFileFromOss(String ossUrl) {
        String fileName = ossUrl.substring(ossUrl.lastIndexOf("/"));
        String filePath = "./";
        File file = saveUrlAs(ossUrl, filePath,fileName,"GET");
        String canonicalPath = "";
        try {
            canonicalPath = file.getCanonicalPath();
        } catch (IOException e) {
            logger.error("获取文件路径失败", e);
        }
        return canonicalPath+fileName;
    }




    /**
     * 下载接口
     * @param filePath 文件将要保存的目录
     * @param fileName 文件名
     * @param method 请求方法，包括POST和GET
     * @param url 请求的路径
     * @return
     */

    public File saveUrlAs(String url, String filePath,String fileName, String method){
        //创建不同的文件夹目录
        File file=new File(filePath);
        //判断文件夹是否存在
        if (!file.exists())
        {
            //如果文件夹不存在，则创建新的的文件夹
            file.mkdirs();
        }
        FileOutputStream fileOut = null;
        HttpURLConnection conn = null;
        InputStream inputStream = null;
        try
        {
            // 建立链接
            URL httpUrl=new URL(url);
            conn=(HttpURLConnection) httpUrl.openConnection();
            //以Post方式提交表单，默认get方式
            conn.setRequestMethod(method);
            conn.setDoInput(true);
            conn.setDoOutput(true);
            // post方式不能使用缓存
            conn.setUseCaches(false);
            //连接指定的资源
            conn.connect();
            //获取网络输入流
            inputStream=conn.getInputStream();
            BufferedInputStream bis = new BufferedInputStream(inputStream);
            //写入到文件（注意文件保存路径的后面一定要加上文件的名称）
            fileOut = new FileOutputStream(filePath+fileName);
            BufferedOutputStream bos = new BufferedOutputStream(fileOut);

            byte[] buf = new byte[4096];
            int length = bis.read(buf);
            //保存文件
            while(length != -1)
            {
                bos.write(buf, 0, length);
                length = bis.read(buf);
            }
            bos.close();
            bis.close();
            conn.disconnect();
        } catch (Exception e)
        {
            logger.error("文件读取失败", e);
        }

        return file;
    }

    /**
     * 删除单个文件
     * @param   sPath 被删除文件path
     * @return 删除成功返回true，否则返回false
     */
    public boolean deleteFile(String sPath) {
        boolean flag = false;
        File file = new File(sPath);
        // 路径为文件且不为空则进行删除
        if (file.isFile() && file.exists()) {
            file.delete();
            flag = true;
        }
        return flag;
    }

    /**
     * 小区id集合转认证小区id集合
     * @param communitySet
     * @return
     */
    private Set<Long> findCommunityExtSet(Collection<Long> communitySet) {
        Set<Long> communityExtSet = Sets.newHashSet();
        List<Long> communityListAll = Lists.newArrayList();
        communityListAll.addAll(communitySet);
        if(!communitySet.isEmpty()) {
            if (communityListAll.size() > ES_MAX_COUNT) {
                List<List<Long>> communityListPartition = Lists.partition(communityListAll, ES_MAX_COUNT);
                for (List<Long> subCommunityList : communityListPartition) {
                    List<Community> communityList = communityApiService.findAll(ApiRequest.newInstance().filterIn(QCommunity.id, subCommunityList));
                    if (!communityList.isEmpty()) {
                        Set<Long> subCommunityExtSet  = communityList.stream().filter(community -> community.getCommunityExtId() > 0L).map(Community::getCommunityExtId).collect(Collectors.toSet());
                        communityExtSet.addAll(subCommunityExtSet);
                    }
                }
            }else{
                List<Community> communityList = communityApiService.findAll(ApiRequest.newInstance().filterIn(QCommunity.id, communitySet));
                if (!communityList.isEmpty()) {
                    communityExtSet = communityList.stream().filter(community -> community.getCommunityExtId() > 0L).map(Community::getCommunityExtId).collect(Collectors.toSet());
                }
            }
        }
        return communityExtSet;
    }

}
