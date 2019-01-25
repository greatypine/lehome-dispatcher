package cn.lehome.dispatcher.utils.operation;

import cn.lehome.base.api.operation.bean.channel.*;
import cn.lehome.base.api.operation.bean.homepage.HomepageModule;
import cn.lehome.base.api.operation.bean.homepage.HomepageModuleItem;
import cn.lehome.base.api.operation.bean.homepage.QHomepageModule;
import cn.lehome.base.api.operation.bean.module.CustomDebrisParam;
import cn.lehome.base.api.operation.bean.module.DebrisModule;
import cn.lehome.base.api.operation.bean.module.DebrisModuleItem;
import cn.lehome.base.api.operation.bean.module.QDebrisModule;
import cn.lehome.base.api.operation.bean.module.debris.DebrisInfo;
import cn.lehome.base.api.operation.bean.module.subassembly.ChannelForwardInfo;
import cn.lehome.base.api.operation.bean.module.subassembly.ChannelInfo;
import cn.lehome.base.api.operation.bean.module.subassembly.QSubassemblyDetail;
import cn.lehome.base.api.operation.bean.module.subassembly.SubassemblyDetail;
import cn.lehome.base.api.operation.service.channel.ChannelApiService;
import cn.lehome.base.api.operation.service.channel.ChannelModuleApiService;
import cn.lehome.base.api.operation.service.homepage.HomepageModuleApiService;
import cn.lehome.base.api.operation.service.module.DebrisModuleApiService;
import cn.lehome.base.api.operation.service.module.DebrisModuleItemApiService;
import cn.lehome.base.api.operation.service.module.debris.DebrisInfoApiService;
import cn.lehome.base.api.operation.service.module.subassembly.SubassemblyDetailApiService;
import cn.lehome.base.api.tool.bean.community.Community;
import cn.lehome.base.api.tool.bean.community.CommunityExt;
import cn.lehome.base.api.tool.bean.community.QCommunity;
import cn.lehome.base.api.tool.bean.region.RegionInfo;
import cn.lehome.base.api.tool.service.community.CommunityApiService;
import cn.lehome.base.api.tool.service.community.CommunityCacheApiService;
import cn.lehome.base.api.tool.service.region.RegionInfoApiService;
import cn.lehome.bean.operation.entity.enums.module.DebrisType;
import cn.lehome.bean.operation.entity.enums.module.SubassemblyType;
import cn.lehome.bean.operation.enums.module.CommunityType;
import cn.lehome.bean.operation.enums.module.DebrisModuleType;
import cn.lehome.bean.operation.enums.module.RangeType;
import cn.lehome.bean.tool.entity.enums.community.EditionType;
import cn.lehome.framework.base.api.core.compoment.loader.LoaderServiceComponent;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.alibaba.dubbo.common.utils.CollectionUtils;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Created by zhanghuan on 2018/10/26.
 */
@Service("operationService")
public class OperationServiceImpl implements OperationService {

    @Autowired
    private HomepageModuleApiService homepageModuleApiService;

    @Autowired
    private LoaderServiceComponent loaderServiceComponent;

    @Autowired
    private DebrisModuleApiService debrisModuleApiService;

    @Autowired
    private CommunityApiService communityApiService;

    @Autowired
    private DebrisModuleItemApiService debrisModuleItemApiService;

    /*@Autowired
    private CommunityCacheApiService communityCacheApiService;*/

    @Autowired
    private DebrisInfoApiService debrisInfoApiService;

    @Autowired
    private ChannelModuleApiService channelModuleApiService;

    @Autowired
    private ChannelApiService channelApiService;

    @Autowired
    private SubassemblyDetailApiService subassemblyDetailApiService;

    @Autowired
    private RegionInfoApiService regionInfoApiService;

    private static final String UNIVERSAL = "UNIVERSAL";
    private static final String COMMUNITY = "COMMUNITY";
    private static final Long COUNTRY = 100000L;

    private static final Long channelIconId = 7L;

    private static final Long wuguanKeyId = 4L;

    private static final Long iconKeyId = 3L;


    @Override
    public void migrateData(String[] input) {
        if (input.length < 1){
            System.out.println("参数错误");
            return;
        }
        String operation = input[1];
        if (operation.equals("migrateHomepageMouleData")){
            this.migrateHomepageMouleData();
        }else if (operation.equals("migrateFragmentData")){
            //this.migrateFragmentData();
        }else if(operation.equals("migrateChannelData")){
            this.migrateChannelData();
        }
    }


    /**
     * channel表数据迁移
     */
    private void migrateChannelData() {

        //迁移channel_module_serve_range数据到debris_module 和通用模板数据
        Set<Long>  countryChannelIdsSet = this.flushDebrisModuleData();
        //迁移 channel_module_item
        this.flushDebrisModuleItemData(countryChannelIdsSet);
    }


    private Set<Long> flushDebrisModuleData(){
        int pageSize = 100;
        int count = 0;
        int pageIndex = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiRequest apiRequest = ApiRequest.newInstance();
        //导入debrisModule并同步 debris_module_item
        ApiResponse<ChannelModuleServeRange> apiResponse = channelModuleApiService.findChannelModuleServeRange(apiRequest,apiRequestPage);
        Set<Long>  countryChannelIdsSet = Sets.newHashSet();
        while (apiResponse.getPagedData().size()>0){
            try {
                count += apiResponse.getPagedData().size();
                List<ChannelModuleServeRange> serveRangeList = Lists.newArrayList(apiResponse.getPagedData());
                if (CollectionUtils.isNotEmpty(serveRangeList)){
                    for (ChannelModuleServeRange channelModuleServeRange : serveRangeList){
                        Long channelModuleId = channelModuleServeRange.getChannelModuleId();
                        //获取模板启用禁用状态
                        ChannelModule channelModule =channelModuleApiService.get(channelModuleId);
                        EnableDisableStatus enableDisableStatus = channelModule.getEnableStatus();
                        //获取服务类型
                        RangeType rangeType =RangeType.COMMUNITY;
                        if (channelModuleServeRange.getIsCommunity().equals(YesNoStatus.NO)){
                            rangeType = this.getRangeTypeByObjectId(channelModuleServeRange.getObjectId());
                        }
                        //保存碎片模板
                        DebrisModule debrisModule = this.saveDebrisModule(DebrisType.ICON,channelIconId,channelModuleServeRange.getObjectId(),"",enableDisableStatus, rangeType);

                        //同步全国通用组件信息到 debris_module_item
                        if (rangeType.equals(RangeType.UNIVERSAL)){
                            this.saveDebrisModuleItem(channelModule.getChannelIds(),debrisModule.getId(),YesNoStatus.NO,channelModuleServeRange.getObjectId());
                            countryChannelIdsSet.addAll(channelModule.getChannelIds());
                        }
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            apiResponse = channelModuleApiService.findChannelModuleServeRange(apiRequest,apiRequestPage);
        }
        System.out.println("通用模板数据共"+countryChannelIdsSet.size());
        System.out.println("迁移debris_module数据完成,共:"+count+"条");
        return countryChannelIdsSet;
    }


    private void flushDebrisModuleItemData(Set<Long>  countryChannelIdsSet){
        int pageSize = 100;
        int count = 0;
        int pageIndex = 0;
        ApiRequest apiRequest = ApiRequest.newInstance();
        //查询去除全国类型的模板数据
        ApiRequestPage debrisModuleApiRequestPage = ApiRequestPage.newInstance();
        debrisModuleApiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<DebrisModule>  debrisModuleApiResponse = debrisModuleApiService.findAll(apiRequest.filterNotEqual(QDebrisModule.objectId,COUNTRY),debrisModuleApiRequestPage);
        while (debrisModuleApiResponse.getPagedData().size()>0){
            try {
                count += debrisModuleApiResponse.getPagedData().size();
                List<DebrisModule> debrisModuleLists = Lists.newArrayList(debrisModuleApiResponse.getPagedData());
                for (DebrisModule debrisModule : debrisModuleLists){
                    ApiResponse<ChannelModuleServeRange>  channelModuleServeRangeApiResponse = channelModuleApiService.findChannelModuleServeRange(ApiRequest.newInstance().filterEqual(QChannelModuleServeRange.objectId,debrisModule.getObjectId()),ApiRequestPage.newInstance());
                    List<Long> channelModuleIds = Lists.newArrayList(channelModuleServeRangeApiResponse.getPagedData()).stream().map(ChannelModuleServeRange::getChannelModuleId).collect(Collectors.toList());
                    if (CollectionUtils.isNotEmpty(channelModuleIds)){
                        ApiResponse<ChannelModule>  channelModuleApiResponse = channelModuleApiService.findAll(ApiRequest.newInstance().filterIn(QChannelModule.id,channelModuleIds),ApiRequestPage.newInstance().paging(0,1000));
                        List<ChannelModule> channelModuleList = Lists.newArrayList(channelModuleApiResponse.getPagedData());
                        YesNoStatus isCommunity = YesNoStatus.NO;
                        if (debrisModule.getRangeType().equals(RangeType.COMMUNITY)){
                            isCommunity = YesNoStatus.YES;
                        }
                        //取出所有需要覆盖的集合
                        List<ChannelModule> coverChannelModuleList = channelModuleList.stream().filter(channelModule -> channelModule.getIsCover().equals(YesNoStatus.YES)).collect(Collectors.toList());
                        if (CollectionUtils.isNotEmpty(coverChannelModuleList)){
                            //有则只取覆盖的数据
                            System.out.println("需要覆盖");
                            loaderServiceComponent.loadAllBatch(coverChannelModuleList,ChannelModule.class);
                            Set<Long> channelIdSet = Sets.newHashSet();
                            coverChannelModuleList.forEach(channelModule ->channelIdSet.addAll(channelModule.getChannelIds()));
                            this.saveDebrisModuleItem(channelIdSet,debrisModule.getId(),isCommunity,debrisModule.getObjectId());
                        }else {
                            //否则需要加上通用模板数据
                            loaderServiceComponent.loadAllBatch(channelModuleList,ChannelModule.class);
                            Set<Long> channelIdSet = Sets.newHashSet();
                            channelModuleList.forEach(channelModule -> channelIdSet.addAll(channelModule.getChannelIds()));
                            channelIdSet.addAll(countryChannelIdsSet);//不覆盖需要将全国channelid的加上
                            this.saveDebrisModuleItem(channelIdSet,debrisModule.getId(),isCommunity,debrisModule.getObjectId());
                        }
                    }

                }
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            pageIndex++;
            debrisModuleApiRequestPage.paging(pageIndex,pageSize);
            debrisModuleApiResponse = debrisModuleApiService.findAll(apiRequest.filterNotEqual(QDebrisModule.objectId,COUNTRY),debrisModuleApiRequestPage);

        }
        System.out.println("迁移debris_module_item数据完成,共:"+count+"条");
    }





    private void saveDebrisModuleItem(Iterable<Long>  channelIds,Long debrisModuleId,YesNoStatus isCommunity,Long objectId){
        Map<String,SubassemblyDetail> stringSubassemblyDetailMap = this.getMapSubassemblyDetail();//获取组件和key的映射关系
        List<DebrisModuleItem> debrisModuleItemList = Lists.newArrayList();
        List<Channel>  channelList = channelApiService.findAll(channelIds).entrySet().stream().map(x->x.getValue()).collect(Collectors.toList());
        for (Channel channel : channelList){
            DebrisModuleItem debrisModuleItem = new DebrisModuleItem();
            SubassemblyDetail subassemblyDetail = stringSubassemblyDetailMap.get(channel.getIconKey());
            debrisModuleItem.setDebrisModuleId(debrisModuleId);
            debrisModuleItem.setComponentId(subassemblyDetail.getId());
            debrisModuleItem.setOrderNumber(Long.valueOf(channel.getPriority()));
            debrisModuleItem.setCreatedTime(channel.getCreatedTime());
            debrisModuleItem.setUpdatedTime(channel.getUpdatedTime());
            this.fillDebrisData(this.getDebrisModuleTypeByObjectId(isCommunity,objectId),debrisModuleItem);
            debrisModuleItemList.add(debrisModuleItem);
        }
        debrisModuleItemApiService.save(debrisModuleItemList);
    }

    private RangeType getRangeTypeByObjectId(Long objectId) {
        RegionInfo regionInfo = regionInfoApiService.get(objectId);
        RangeType rangeType;
        switch (regionInfo.getType()) {
            case COUNTRY:
                rangeType = RangeType.UNIVERSAL;
                break;
            case PROVINCE:
                rangeType = RangeType.PROVINCE;
                break;
            case CITY:
                rangeType = RangeType.CITY;
                break;
            default:
                rangeType = RangeType.COMMUNITY;
                break;
        }
        return rangeType;
    }

    /**
     * 获取组件和key的映射
     * @return subassemblyDetailMap
     */
    private Map<String,SubassemblyDetail> getMapSubassemblyDetail(){
        ApiResponse<SubassemblyDetail>  subassemblyDetailApiResponse = subassemblyDetailApiService.findAll(ApiRequest.newInstance().filterEqual(QSubassemblyDetail.subassemblyType, SubassemblyType.ICON),ApiRequestPage.newInstance().paging(0,1000));
        Map<String,SubassemblyDetail> subassemblyDetailMap = Lists.newArrayList(subassemblyDetailApiResponse.getPagedData()).stream().collect(Collectors.toMap(SubassemblyDetail::getSubassemblyKey,subassemblyDetail -> subassemblyDetail));
        return subassemblyDetailMap;
    }


    @Override
    public void flushIconDate() {

        List<Channel> channelList = channelApiService.findAll(ApiRequest.newInstance());

        List<ChannelInfo> list = new ArrayList<>();
        for (Channel channel : channelList){
            ChannelInfo channelInfo = BeanMapping.map(channel, ChannelInfo.class);
            ChannelForwardInfo channelForwardInfo = JSON.parseObject(channel.getTargetValue(), ChannelForwardInfo.class);
            channelInfo.setChannelForwardInfo(channelForwardInfo);
            list.add(channelInfo);
        }

        subassemblyDetailApiService.flushIconData(list);


    }


    /**
     *homepageModule表数据迁移
     */
    private void migrateHomepageMouleData() {
        int pageSize = 100;
        int count = 0;
        int pageIndex = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiRequest apiRequest = ApiRequest.newInstance().filterNotEqual(QHomepageModule.communityExtId,0L);
        ApiResponse<HomepageModule> response = homepageModuleApiService.findAll(apiRequest,apiRequestPage);
        while(response.getPagedData().size()>0){
            try {
                count += response.getPagedData().size();
                List<HomepageModule> homepageModuleList = Lists.newArrayList(response.getPagedData());
                loaderServiceComponent.loadAllBatch(homepageModuleList,HomepageModule.class);

                for (HomepageModule homepageModule : homepageModuleList){
                    if (CollectionUtils.isNotEmpty(homepageModule.getHomepageModuleItemList())){
                        //icon数据迁移
                        this.flushChannelData(homepageModule,iconKeyId);
                    }
                    //物管公告数据迁移
                    this.flushCustomParamData(homepageModule,wuguanKeyId);
                }

            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = homepageModuleApiService.findAll(apiRequest,apiRequestPage);
        }
        System.out.println("homepageMouleData 数据处理完毕 " + count);
    }


    /**
     * 自定义模板参数迁移 物管公告是否显示数据
     * @param homepageModule 迁移实体
     * @param debrisId 碎片ID
     */
    private void flushCustomParamData(HomepageModule homepageModule,Long debrisId){
        List<CustomDebrisParam> customDebrisParamList =Lists.newArrayList();
        DebrisModule debrisModule  = this.saveDebrisModuleHomePageModule(DebrisType.CUSTOM,debrisId,homepageModule.getCommunityExtId(),homepageModule.getName(),homepageModule.getEnableStatus(),RangeType.COMMUNITY);
        CustomDebrisParam customDebrisParam = new CustomDebrisParam();
        customDebrisParam.setDebrisModuleId(debrisModule.getId());
        customDebrisParam.setDebrisModuleType(this.getDebrisModuleTypeByCommunityExtId(homepageModule.getCommunityExtId()));
        customDebrisParam.setParamId(1L);
        System.out.println(homepageModule.getIsDisplayNotice().toString());
        customDebrisParam.setParamValue(homepageModule.getIsDisplayNotice().toString());
        customDebrisParamList.add(customDebrisParam);
        debrisModule.setCustomDebrisParamList(customDebrisParamList);
        debrisModule.setDebrisModuleItemList(Lists.newArrayList());
        debrisModuleApiService.saveDebrisModuleItemAndParam(debrisModule,debrisModule.getId());
    }



    /**
     * 获取碎片模板类型
     * @param communityExtId 绑定小区ID
     * @return DebrisModuleType
     */
    private DebrisModuleType getDebrisModuleTypeByCommunityExtId (Long communityExtId){
        CommunityExt communityExt = communityApiService.getExt(communityExtId);
        //CommunityExt communityExt = communityCacheApiService.getCommunityExt(communityExtId);
        return communityExt.getEditionType().equals(EditionType.free) ? DebrisModuleType.WG : DebrisModuleType.ZSQ;
    }

    /**
     * 通过objectID获取碎片模板类型
     * @param isCommunity 是否小区
     * @param objectId 区域ID
     * @return DebrisModuleType
     */
    private DebrisModuleType getDebrisModuleTypeByObjectId(YesNoStatus isCommunity,Long objectId){
        if (YesNoStatus.YES.equals(isCommunity)){
            //return DebrisModuleType.KF;
            Community community = communityApiService.get(objectId);
            loaderServiceComponent.load(community,QCommunity.communityExt);
            if (community == null){
                System.out.println("小区信息未找到");
                return DebrisModuleType.KF;
            }
            return community.getCommunityExt() == null ? DebrisModuleType.KF : this.getDebrisModuleTypeByCommunityExtId(community.getCommunityExtId());
        }else { 
            return DebrisModuleType.UNIVERSAL;
        }
    }

    /**
     * 填充碎片模板与组件关系表数据
     * @param debrisModuleItem 碎片模板与组件关系实体
     */
    private void fillDebrisData (DebrisModuleType debrisModuleType,DebrisModuleItem debrisModuleItem ){
        switch (debrisModuleType){
            case KF:
                debrisModuleItem.setKfDisplay(YesNoStatus.YES);
                debrisModuleItem.setWgDisplay(YesNoStatus.NO);
                debrisModuleItem.setZsqDisplay(YesNoStatus.NO);
                break;
            case WG:
                debrisModuleItem.setKfDisplay(YesNoStatus.NO);
                debrisModuleItem.setWgDisplay(YesNoStatus.YES);
                debrisModuleItem.setZsqDisplay(YesNoStatus.NO);
                break;
            case ZSQ:
                debrisModuleItem.setKfDisplay(YesNoStatus.NO);
                debrisModuleItem.setWgDisplay(YesNoStatus.NO);
                debrisModuleItem.setZsqDisplay(YesNoStatus.YES);
                break;
            default:
                debrisModuleItem.setKfDisplay(YesNoStatus.YES);
                debrisModuleItem.setWgDisplay(YesNoStatus.YES);
                debrisModuleItem.setZsqDisplay(YesNoStatus.YES);
                break;
        }
    }


    /**
     * 保存碎片模板
     * @param debrisType 碎片类型
     * @param debrisId 碎片ID
     * @param objectId 小区ID
     * @return DebrisModule 碎片模板
     */
    private DebrisModule saveDebrisModule(DebrisType debrisType, Long debrisId, Long objectId, String moduleName, EnableDisableStatus enableStatus ,RangeType rangeType){
        DebrisModule debrisModule = new DebrisModule();
        debrisModule.setDebrisType(debrisType);
        debrisModule.setDebrisId(debrisId);
        debrisModule.setEnableStatus(enableStatus);
        debrisModule.setName(moduleName);
        debrisModule.setRangeType(rangeType);
        debrisModule.setObjectIdList(Lists.newArrayList(objectId));
        if (rangeType.equals(RangeType.COMMUNITY)){
            Community community = communityApiService.get(objectId);
            loaderServiceComponent.load(community,QCommunity.communityExt);
            if (community == null){
                debrisModule.setObjectId(objectId);
                debrisModule.setCommunityType(CommunityType.OPEN);
                System.out.println("小区信息未找到");
            }else {
                CommunityType communityType = community.getCommunityExt() == null ? CommunityType.OPEN : CommunityType.CERTIFICATE;
                Long communityId = community.getCommunityExt() == null ? objectId :community.getCommunityExtId();
                debrisModule.setObjectId(communityId);
                debrisModule.setCommunityType(communityType);
            }

        }else {
            debrisModule.setCommunityType(CommunityType.NONE);
            debrisModule.setObjectId(objectId);
        }
        this.fillDebrisModuleKeyAndNameAndCommunityType(debrisModule,YesNoStatus.NO);
        debrisModule = debrisModuleApiService.save(debrisModule);
        return debrisModule;
    }

    /**
     * 获取小区ID
     * @param homepageModule 首页模板实体
     * @return communityId
     */
    private Long getObjectId(HomepageModule homepageModule){
        Long communityExtId = homepageModule.getCommunityExtId();
        List<Community> communityList = communityApiService.findAll(ApiRequest.newInstance().filterEqual(QCommunity.communityExtId,communityExtId));
        if (CollectionUtils.isNotEmpty(communityList)){
            return communityList.get(0).getId();
        }
        return 0L;
    }


    /**
     * 迁移channel表icon数据
     * @param homepageModule 首页模板实体
     * @param debrisId 碎片ID
     */
    private void flushChannelData(HomepageModule homepageModule,Long debrisId) {
        //Long objetId = this.getObjectId(homepageModule);

        DebrisModule debrisModule  = this.saveDebrisModuleHomePageModule(DebrisType.ICON,debrisId,homepageModule.getCommunityExtId(),homepageModule.getName(),homepageModule.getEnableStatus(),RangeType.COMMUNITY);
        Set<Long> channelIds = homepageModule.getHomepageModuleItemList().stream().map(HomepageModuleItem::getChannelId).collect(Collectors.toSet());
        Map<Long, Channel> channelMap = channelApiService.findAll(channelIds);
        Map<Long,Integer> orderMap = Maps.newHashMap();
        homepageModule.getHomepageModuleItemList().forEach(homepageModuleItem -> {
            orderMap.put(homepageModuleItem.getChannelId(),homepageModuleItem.getOrderNumber());
        });
        List<Channel> channelList = channelMap.entrySet().stream().map(x->x.getValue()).collect(Collectors.toList());
        Map<String,SubassemblyDetail> subassemblyDetailMap = this.getMapSubassemblyDetail();
        List<DebrisModuleItem> debrisModuleItemList = Lists.newArrayList();
        for (Channel channel : channelList){
            DebrisModuleItem debrisModuleItem = new DebrisModuleItem();
            SubassemblyDetail subassemblyDetail = subassemblyDetailMap.get(channel.getIconKey());
            debrisModuleItem.setDebrisModuleId(debrisModule.getId());
            debrisModuleItem.setComponentId(subassemblyDetail.getId());
            Integer orderNumber =  orderMap.get(channel.getId());
            debrisModuleItem.setOrderNumber(Long.valueOf(orderNumber));
            debrisModuleItem.setCreatedTime(channel.getCreatedTime());
            debrisModuleItem.setUpdatedTime(channel.getUpdatedTime());
            this.fillDebrisData(this.getDebrisModuleTypeByCommunityExtId(homepageModule.getCommunityExtId()),debrisModuleItem);
            debrisModuleItemList.add(debrisModuleItem);
        }
        debrisModuleItemApiService.save(debrisModuleItemList);
    }



    /**
     * 保存碎片模板
     * @param debrisType 碎片类型
     * @param debrisId 碎片ID
     * @param objectId 小区ID
     * @return DebrisModule 碎片模板
     */
    private DebrisModule saveDebrisModuleHomePageModule(DebrisType debrisType, Long debrisId, Long objectId, String moduleName, EnableDisableStatus enableStatus ,RangeType rangeType){
        DebrisModule debrisModule = new DebrisModule();
        debrisModule.setDebrisType(debrisType);
        debrisModule.setDebrisId(debrisId);
        debrisModule.setEnableStatus(enableStatus);
        debrisModule.setName(moduleName);
        debrisModule.setRangeType(rangeType);
        debrisModule.setObjectIdList(Lists.newArrayList(objectId));
        debrisModule.setCommunityType(CommunityType.CERTIFICATE);
        debrisModule.setObjectId(objectId);
        this.fillDebrisModuleKeyAndNameAndCommunityType(debrisModule,YesNoStatus.YES);
        debrisModule = debrisModuleApiService.save(debrisModule);
        return debrisModule;
    }



    public void fillDebrisModuleKeyAndNameAndCommunityType(DebrisModule debrisModule,YesNoStatus isExt) {
        DebrisInfo debrisInfo = debrisInfoApiService.get(debrisModule.getDebrisId());
        //按照应用范围+碎片类型
        String rangeName = "";
        debrisModule.setCommunityType(CommunityType.NONE);
        switch (debrisModule.getRangeType().name()) {
            case UNIVERSAL:
                rangeName = RangeType.UNIVERSAL.getCode();
                break;
            case COMMUNITY:
                if (isExt.equals(YesNoStatus.NO)){
                    Community community = communityApiService.get(debrisModule.getObjectId());
                    //Community community = communityCacheApiService.getCommunity(debrisModule.getObjectId());
                    rangeName = community == null ? "" : community.getName();
                    if(community != null && community.getCommunityExtId() > 0L) {
                        debrisModule.setCommunityType(CommunityType.CERTIFICATE);
                        //如果是认证小区,存认证小区的id
                        debrisModule.setObjectId(community.getCommunityExtId());
                        loaderServiceComponent.load(community, QCommunity.communityExt);
                        rangeName = community.getCommunityExt() == null ? "" : community.getCommunityExt().getName();
                    }else{
                        debrisModule.setCommunityType(CommunityType.OPEN);
                    }
                }else {
                    CommunityExt communityExt = communityApiService.getExt(debrisModule.getObjectId());
                    rangeName = communityExt.getName() == null ?"" : communityExt.getName();
                    debrisModule.setObjectId(communityExt.getId());
                }

                break;
            default:
                RegionInfo regionInfo = regionInfoApiService.get(debrisModule.getObjectId());
                rangeName = regionInfo == null ? "" : regionInfo.getName();
                break;
        }
        //生成和设置key
        String key = debrisModule.getRangeType().name() + "_" + debrisInfo.getDebrisKey() + "_" + debrisModule.getObjectId();
        debrisModule.setKey(key);

        String name = "";
        if (debrisInfo != null) {
            name = rangeName + "-" + debrisInfo.getDebrisName() + "_" + debrisInfo.getDebrisType().getName();
        }
        debrisModule.setName(name);
    }
}
