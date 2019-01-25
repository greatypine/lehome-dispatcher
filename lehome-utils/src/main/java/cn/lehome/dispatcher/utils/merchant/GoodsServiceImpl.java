package cn.lehome.dispatcher.utils.merchant;

import cn.lehome.base.api.business.bean.ecommerce.goods.GoodsCatalog;
import cn.lehome.base.api.business.bean.ecommerce.goods.GoodsSpuIndex;
import cn.lehome.base.api.business.bean.ecommerce.goods.QGoodsSpuIndex;
import cn.lehome.base.api.business.bean.response.goods.GoodsInfo;
import cn.lehome.base.api.business.service.ecommerce.goods.GoodsCatalogApiService;
import cn.lehome.base.api.business.service.ecommerce.goods.GoodsSpuIndexApiService;
import cn.lehome.base.api.business.service.goods.GoodsInfoApiService;
import cn.lehome.base.api.business.service.goods.GoodsInfoIndexApiService;
import cn.lehome.bean.business.enums.ecommerce.goods.GoodsType;
import cn.lehome.bean.business.enums.ecommerce.goods.SaleStatus;
import cn.lehome.bean.business.merchant.search.GoodsInfoIndexEntity;
import cn.lehome.dispatcher.utils.es.util.EsFlushUtil;
import cn.lehome.framework.base.api.core.compoment.request.ApiPageRequestHelper;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by zhuhai on 2018/8/21
 */
@Service("goodsService")
public class GoodsServiceImpl implements GoodsService{

    @Autowired
    private GoodsInfoApiService goodsInfoApiService;

    @Autowired
    private GoodsInfoIndexApiService goodsInfoIndexApiService;

    @Autowired
    private GoodsSpuIndexApiService goodsSpuIndexApiService;

    @Autowired
    private ThreadPoolExecutor userTaskThreadPool;

    @Autowired
    private GoodsCatalogApiService goodsCatalogApiService;


    @Override
    public void initMerchantIndex(String input[]) {
        if(input.length < 2){
            System.out.println("参数错误");
            return;
        }
        String option = input[1];
        int pageNo = 0;
        if(input.length > 2){
            pageNo = Integer.valueOf(input[2]);
        }
        if("initGoodsIndex".equals(option)) {
            this.initGoodsInfoIndex(pageNo);
        }else if ("deleteGoodsInfoIndex".equals(option)){
            this.deleteGoodsInfoIndex(pageNo);
        }else{
            System.out.println("参数错误");
        }
    }

    @Override
    public void refreshGoodsNum(String[] input) {
        int pageIndex = 0;
        int pageSize = 20;
        if (input.length == 2 ) {
            pageIndex = Integer.valueOf(input[1]);
        }
        if (input.length == 3 ) {
            pageSize = Integer.valueOf(input[1]);
        }
        ApiRequest apiRequest = ApiRequest.newInstance().filterLikes(QGoodsSpuIndex.saleStatus, Lists.newArrayList(SaleStatus.SHELF.name(), SaleStatus.SHELVES.name()))
                .filterLikes(QGoodsSpuIndex.goodsType, Lists.newArrayList(GoodsType.NORMAL.name(), GoodsType.SHOPGIFT.name()));
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance().paging(pageIndex,pageSize);
        List<GoodsSpuIndex> goodsSpuIndexList = ApiPageRequestHelper.request(apiRequest, apiRequestPage, goodsSpuIndexApiService::findAll);

        Map<Long, Integer> cataLogGoodsNumMap = Maps.newHashMap();
        goodsSpuIndexList.forEach(e -> {
            cataLogGoodsNumMap.put(e.getFirstGoodsCatalogId(), cataLogGoodsNumMap.get(e.getFirstGoodsCatalogId())==null ? 1 : (cataLogGoodsNumMap.get(e.getFirstGoodsCatalogId())+1));
            cataLogGoodsNumMap.put(e.getSecondGoodsCatalogId(), cataLogGoodsNumMap.get(e.getSecondGoodsCatalogId())==null ? 1 : (cataLogGoodsNumMap.get(e.getSecondGoodsCatalogId())+1));
        });
        cataLogGoodsNumMap.keySet().parallelStream().forEach(e -> updateCatalogGoodsNum(e, cataLogGoodsNumMap.get(e)));
    }

    private void updateCatalogGoodsNum(Long goodsCatalogId, Integer goodsNum) {
        GoodsCatalog goodsCatalog = goodsCatalogApiService.get(goodsCatalogId);
        goodsCatalog.setGoodsNum(goodsNum);
        goodsCatalogApiService.update(goodsCatalog);
    }

    private void deleteGoodsInfoIndex(int pageIndex) {
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<GoodsInfo> response = goodsInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while (response.getPagedData().size()>0){
            count += response.getPagedData().size();
            try {
                List<GoodsInfoIndexEntity> goodsInfoIndexEntities = BeanMapping.mapList(Lists.newArrayList(response.getPagedData()), GoodsInfoIndexEntity.class);
                EsFlushUtil.getInstance().batchDelete(goodsInfoIndexEntities);
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }

            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = goodsInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("共删除goodsinfoIndex缓存数据:"+count+"条");
    }


    private void initGoodsInfoIndex(int pageIndex){
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<GoodsInfo> response = goodsInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            final Collection<GoodsInfo> collection = response.getPagedData();
            userTaskThreadPool.execute(() -> collection.forEach(c -> {
                GoodsInfo goodsInfo = goodsInfoApiService.get(c.getId());
                goodsInfoIndexApiService.saveOrUpdate(goodsInfo);
            }));
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = goodsInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
            System.out.println(pageIndex);
        }
        while (userTaskThreadPool.getActiveCount() != 0) {
            try {
                System.out.println("initGoodsIndex 数据加载完毕" + count + "，还有" + userTaskThreadPool.getQueue().size() + "任务等执行");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("initGoodsIndex 数据处理完毕 " + count);
    }

}
