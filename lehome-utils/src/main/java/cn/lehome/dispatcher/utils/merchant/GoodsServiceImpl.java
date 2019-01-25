package cn.lehome.dispatcher.utils.merchant;

import cn.lehome.base.api.business.bean.response.goods.GoodsInfo;
import cn.lehome.base.api.business.service.goods.GoodsInfoApiService;
import cn.lehome.base.api.business.service.goods.GoodsInfoIndexApiService;
import cn.lehome.bean.business.merchant.search.GoodsInfoIndexEntity;
import cn.lehome.dispatcher.utils.es.util.EsFlushUtil;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
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
    private ThreadPoolExecutor userTaskThreadPool;


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
