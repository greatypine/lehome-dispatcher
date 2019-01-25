package cn.lehome.dispatcher.utils.ecommerce;

import cn.lehome.base.api.business.bean.ecommerce.goods.*;
import cn.lehome.base.api.business.bean.ecommerce.order.*;
import cn.lehome.base.api.business.bean.ecommerce.pay.PayRecord;
import cn.lehome.base.api.business.bean.ecommerce.store.QStoreGoodsRelationship;
import cn.lehome.base.api.business.bean.ecommerce.store.Store;
import cn.lehome.base.api.business.bean.ecommerce.store.StoreGoodsRelationship;
import cn.lehome.base.api.business.service.ecommerce.goods.*;
import cn.lehome.base.api.business.service.ecommerce.order.GoodsGroupbuyApiService;
import cn.lehome.base.api.business.service.ecommerce.order.OrderApiService;
import cn.lehome.base.api.business.service.ecommerce.order.OrderBackApiService;
import cn.lehome.base.api.business.service.ecommerce.order.OrderDetailApiService;
import cn.lehome.base.api.business.service.ecommerce.pay.PayRecordApiService;
import cn.lehome.base.api.business.service.ecommerce.store.StoreApiService;
import cn.lehome.base.api.business.service.ecommerce.store.StoreGoodsRelationshipApiService;
import cn.lehome.base.api.oauth2.bean.user.UserAccount;
import cn.lehome.base.api.oauth2.bean.user.UserAccountDetails;
import cn.lehome.base.api.oauth2.service.user.UserAccountApiService;
import cn.lehome.bean.business.entity.search.ecommerce.goods.GoodsCatalogIndexEntity;
import cn.lehome.bean.business.entity.search.ecommerce.goods.GoodsSkuIndexEntity;
import cn.lehome.bean.business.entity.search.ecommerce.goods.GoodsSpuIndexEntity;
import cn.lehome.bean.business.entity.search.ecommerce.order.GoodsGroupbuyIndexEntity;
import cn.lehome.bean.business.entity.search.ecommerce.order.OrderBackIndexEntity;
import cn.lehome.bean.business.entity.search.ecommerce.order.OrderDetailIndexEntity;
import cn.lehome.bean.business.entity.search.ecommerce.order.OrderIndexEntity;
import cn.lehome.bean.business.entity.search.ecommerce.pay.PayRecordIndexEntity;
import cn.lehome.bean.business.enums.ecommerce.goods.UrlType;
import cn.lehome.dispatcher.utils.es.util.EsFlushUtil;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by zhanghuan on 2018/11/29.
 */
@Service("ecommerceService")
public class EcommerceServiceImpl implements EcommerceService{


    @Autowired
    private GoodsSkuApiService goodsSkuApiService;

    @Autowired
    private GoodsSpuApiService goodsSpuApiService;

    @Autowired
    private GoodsCatalogApiService goodsCatalogApiService;

    @Autowired
    private GoodsResourceApiService goodsResourceApiService;

    @Autowired
    private GoodsCatalogRelationshipService goodsCatalogRelationshipService;

    @Autowired
    private StoreGoodsRelationshipApiService storeGoodsRelationshipApiService;

    @Autowired
    private OrderApiService orderApiService;

    @Autowired
    private OrderDetailApiService orderDetailApiService;

    @Autowired
    private StoreApiService storeApiService;

    @Autowired
    private PayRecordApiService payRecordApiService;

    @Autowired
    private UserAccountApiService userAccountApiService;

    @Autowired
    private GoodsGroupbuyApiService goodsGroupbuyApiService;

    @Autowired
    private OrderBackApiService orderBackApiService;


    @Override
    public void updateEcommerceData(String [] input) {
        if(input.length < 2){
            System.out.println("参数错误");
            return;
        }
        String option = input[1];
        if(input.length > 2){
            System.out.println("参数错误");
            return;
        }
        if ("updateGoodsInfoIndex".equals(option)){
            this.updateGoodsInfoIndex();
        }
        if ("updateOrderInfoIndex".equals(option)){
            this.flushIndex();
        }
    }




    private void updateGoodsInfoIndex() {
        //刷新分类
        this.flushGoodsCataLogIndex();
        //刷新商品
        this.flushGoodsSpuAndSkuIndex();
    }

    private void flushGoodsSpuAndSkuIndex() {
        int count = 0;
        int pageIndex = 0;
        int pageSize = 100;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<GoodsSpu> response = goodsSpuApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        System.out.println("刷新goodsSpu和goodsSku开始...");
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            List<GoodsSpu> goodsSpus = Lists.newArrayList(response.getPagedData());
            List<GoodsSpuIndexEntity> goodsSpuIndexEntities = Lists.newArrayList();
            List<GoodsSkuIndexEntity> goodsSkuIndexEntities = Lists.newArrayList();
            goodsSpus.forEach(goodsSpu -> {
                GoodsSpuIndex goodsSpuIndex = BeanMapping.map(goodsSpu,GoodsSpuIndex.class);
                Long goodsId = goodsSpu.getId();
                List<GoodsResource> goodsResourceList = goodsResourceApiService.findByObjectId(goodsId);
                //商品图片
                List<GoodsResource> goodsImages = goodsResourceList.stream().filter(goodsResource -> UrlType.GOODSIMAGE.equals(goodsResource.getUrlType())).collect(Collectors.toList());
                //商品分享图片
                List<GoodsResource> coverImages = goodsResourceList.stream().filter(goodsResource -> UrlType.COVERIMAGE.equals(goodsResource.getUrlType())).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(goodsImages)){
                    List<String> goodsImageUrls = goodsImages.stream().map(GoodsResource::getSourceUrl).collect(Collectors.toList());
                    goodsSpuIndex.setGoodsImageUrls(goodsImageUrls);
                }
                if (CollectionUtils.isNotEmpty(coverImages)){
                    List<String> coverImageUrls = coverImages.stream().map(GoodsResource::getSourceUrl).collect(Collectors.toList());
                    goodsSpuIndex.setCoverImageUrl(coverImageUrls.get(0));//分享图只有一张
                }

                //商品分类与商品关系信息
                GoodsCatalogRelationship goodsCatalogRelationship = goodsCatalogRelationshipService.findByGoodsId(goodsId);
                if (goodsCatalogRelationship != null){
                    Long firstGoodsCatalogId = goodsCatalogRelationship.getFirstGoodsCatalogId();
                    Long secondGoodsCatalogId = goodsCatalogRelationship.getSecondGoodsCatalogId();
                    GoodsCatalog firstGoodsCatalog = goodsCatalogApiService.get(firstGoodsCatalogId);
                    GoodsCatalog secondGoodsCatalog = goodsCatalogApiService.get(secondGoodsCatalogId);
                    goodsSpuIndex.setFirstGoodsCatalogId(firstGoodsCatalogId);
                    goodsSpuIndex.setFirstGoodsCatalogName(firstGoodsCatalog.getCatalogName());
                    goodsSpuIndex.setSecondGoodsCatalogName(secondGoodsCatalog.getCatalogName());
                    goodsSpuIndex.setSecondGoodsCatalogId(secondGoodsCatalogId);
                }

                //不卖这件商品的店铺
                List<StoreGoodsRelationship> storeGoodsRelationships = storeGoodsRelationshipApiService.findAll(ApiRequest.newInstance().filterEqual(QStoreGoodsRelationship.goodsId,goodsId));
                if (CollectionUtils.isNotEmpty(storeGoodsRelationships)){
                    List<Long> excludeStoreIds =  storeGoodsRelationships.stream().map(StoreGoodsRelationship::getStoreId).collect(Collectors.toList());
                    goodsSpuIndex.setExcludeStoreIds(excludeStoreIds);
                }else {
                    //默认都卖
                    goodsSpuIndex.setExcludeStoreIds(Lists.newArrayList());
                }

                //商品默认价格
                GoodsSku defaultGoodsSku = goodsSkuApiService.findDefaultSku(goodsId);
                goodsSpuIndex.setDefaultSkuPrice(defaultGoodsSku.getGoodsPrice());

                GoodsSpuIndexEntity goodsSpuIndexEntity = BeanMapping.map(goodsSpuIndex, GoodsSpuIndexEntity.class);
                if (goodsSpuIndexEntity.getBrokerageRate() == null) {
                    goodsSpuIndexEntity.setBrokerageRate(BigDecimal.ZERO);
                }
                goodsSpuIndexEntities.add(goodsSpuIndexEntity);


                //商品规格信息
                ApiResponse<GoodsSku>  goodsSkuApiResponse = goodsSkuApiService.findAll(ApiRequest.newInstance().filterEqual(QGoodsSku.goodsId,goodsId),ApiRequestPage.newInstance().paging(0,100));
                List<GoodsSku> goodsSkus = Lists.newArrayList(goodsSkuApiResponse.getPagedData());
                List<GoodsSkuIndexEntity> goodsSkuIndexEntities1 = BeanMapping.mapList(goodsSkus,GoodsSkuIndexEntity.class);
                goodsSkuIndexEntities1.forEach(goodsSku -> {
                    goodsSkuIndexEntities.add(goodsSku);
                });
            });
            EsFlushUtil.getInstance().batchInsert(goodsSpuIndexEntities);
            EsFlushUtil.getInstance().batchInsertChild(goodsSkuIndexEntities,QGoodsSkuIndex.goodsId);

            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = goodsSpuApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("刷新goodsSpu和goodsSku执行完毕" + "共" + count + "条商品数据");
    }

    private void flushGoodsCataLogIndex() {
        int count = 0;
        int pageIndex = 0;
        int pageSize = 100;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<GoodsCatalog>  response = goodsCatalogApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        System.out.println("刷新商品分类索引信息开始...");
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            List<GoodsCatalog> goodsCatalogList = Lists.newArrayList(response.getPagedData());
            List<GoodsCatalogIndexEntity> goodsCatalogIndexEntityList= BeanMapping.mapList(goodsCatalogList,GoodsCatalogIndexEntity.class);
            EsFlushUtil.getInstance().batchInsert(goodsCatalogIndexEntityList);
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = goodsCatalogApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("刷新goodsCatalogIndex执行完毕" + "共" + count + "条数据");
    }

    private void flushIndex() {
        this.flushOrderIndex();
        this.flushOrderDetailIndex();
        this.flushOrderBackIndex();
        this.flushGoodsGroupbuyIndex();
        this.flushPayRecordIndex();
    }

    private void flushOrderIndex(){
        int pageIndex = 0;
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<Order> response = orderApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            try {
                List<OrderIndexEntity> orderIndexEntityList = BeanMapping.mapList(Lists.newArrayList(response.getPagedData()), OrderIndexEntity.class);
                orderIndexEntityList.stream().forEach(entity -> {
                    Store store = storeApiService.findOne(entity.getStoreId());
                    entity.setStoreName(store.getStoreName());
                    entity.setStoreType(store.getStoreType());
                    entity.setStoreUserId(store.getUserAccountId());
                    UserAccount userAccount = userAccountApiService.get(entity.getUserAccountId());
                    entity.setStoreUserPhone(userAccount.getPhone());
                    PayRecord payRecord = payRecordApiService.findOne(entity.getPayRecordId());
                    if (payRecord != null){
                        entity.setPayType(payRecord.getPayType());
                    }
                });


                EsFlushUtil.getInstance().batchInsert(orderIndexEntityList);
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = orderApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("刷新order执行完毕" + "共" + count + "条数据");
    }



    private void flushOrderDetailIndex(){
        int pageIndex = 0;
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<OrderDetail> response = orderDetailApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            try {
                List<OrderDetailIndexEntity> orderDetailIndexEntityList = BeanMapping.mapList(Lists.newArrayList(response.getPagedData()), OrderDetailIndexEntity.class);
                orderDetailIndexEntityList.stream().forEach(entity -> {
                    OrderBack orderBack = orderBackApiService.findByOrderDetailNo(entity.getOrderNo());
                    if (orderBack != null){
                        entity.setStatus(orderBack.getStatus());
                    }
                });

                EsFlushUtil.getInstance().batchInsertChild(orderDetailIndexEntityList, QOrderDetailIndex.orderId);
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = orderDetailApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("刷新orderDetail执行完毕" + "共" + count + "条数据");
    }


    private void flushGoodsGroupbuyIndex(){
        int pageIndex = 0;
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<GoodsGroupbuy> response = goodsGroupbuyApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            try {
                List<GoodsGroupbuyIndexEntity> goodsGroupbuyIndexEntityList = BeanMapping.mapList(Lists.newArrayList(response.getPagedData()), GoodsGroupbuyIndexEntity.class);
                EsFlushUtil.getInstance().batchInsert(goodsGroupbuyIndexEntityList);
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = goodsGroupbuyApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("刷新goodsGroupbuy执行完毕" + "共" + count + "条数据");
    }


    private void flushOrderBackIndex(){
        int pageIndex = 0;
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<OrderBack> response = orderBackApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            try {
                List<OrderBackIndexEntity> orderBackIndexEntityList = BeanMapping.mapList(Lists.newArrayList(response.getPagedData()), OrderBackIndexEntity.class);
                EsFlushUtil.getInstance().batchInsert(orderBackIndexEntityList);
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = orderBackApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("刷新orderBack执行完毕" + "共" + count + "条数据");
    }

    private void flushPayRecordIndex(){
        int pageIndex = 0;
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<PayRecord> response = payRecordApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            try {
                List<PayRecordIndexEntity> payRecordIndexEntityList = BeanMapping.mapList(Lists.newArrayList(response.getPagedData()), PayRecordIndexEntity.class);
                payRecordIndexEntityList.stream().forEach(entity -> {
                    UserAccountDetails userAccountDetails = userAccountApiService.getUserAccountDetails(entity.getUserAccountId());
                    if (userAccountDetails != null){
                        entity.setUserPhone(userAccountDetails.getPhone());
                        entity.setUserName(userAccountDetails.getNickName());
                    }
                });

                EsFlushUtil.getInstance().batchInsert(payRecordIndexEntityList);
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = payRecordApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("刷新payRecord执行完毕" + "共" + count + "条数据");
    }




}
