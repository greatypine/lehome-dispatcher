package cn.lehome.dispatcher.quartz.service.invoke.ecommerce;

import cn.lehome.base.api.business.bean.ecommerce.goods.GoodsSpuIndex;
import cn.lehome.base.api.business.bean.ecommerce.goods.QGoodsSpuIndex;
import cn.lehome.base.api.business.service.ecommerce.goods.GoodsSpuApiService;
import cn.lehome.base.api.business.service.ecommerce.goods.GoodsSpuIndexApiService;
import cn.lehome.bean.business.enums.ecommerce.goods.SaleStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service("putOnShelvesScheduleJobService")
public class PutOnShelvesScheduleJobServiceImpl extends AbstractInvokeServiceImpl{

    @Autowired
    private GoodsSpuApiService goodsSpuApiService;

    @Autowired
    private GoodsSpuIndexApiService goodsSpuIndexApiService;

    @Override
    public void doInvoke(Map<String, String> params) {

        logger.error("进入商品上架定时任务");
        boolean flag = true;
        int pageSize = 100;
        Long maxId = 0L;

        while(flag){
            try{
                ApiResponse<GoodsSpuIndex> apiResponse = goodsSpuIndexApiService.findAll(ApiRequest.newInstance().filterLike(QGoodsSpuIndex.saleStatus, SaleStatus.SHELF).filterLessEqual(QGoodsSpuIndex.saleStartTime, new Date().getTime()).filterGreaterThan(QGoodsSpuIndex.id, maxId), ApiRequestPage.newInstance().paging(0, pageSize).addOrder(QGoodsSpuIndex.id, PageOrderType.ASC));

                if (!CollectionUtils.isEmpty(apiResponse.getPagedData())){
                    List<GoodsSpuIndex> goodsSpuIndexList = Lists.newArrayList(apiResponse.getPagedData());
                    goodsSpuIndexList.stream().forEach(goodsSpuIndex -> {
                        try {
                            goodsSpuApiService.updateGoodsStatusForCMS(Long.valueOf(goodsSpuIndex.getId()),SaleStatus.SHELVES);
                        }catch (Exception e){
                            logger.error("商品" + goodsSpuIndex.getId() + "上架失败");
                        }
                    });

                    maxId = Long.valueOf(goodsSpuIndexList.get(goodsSpuIndexList.size() - 1).getId());
                }else{
                    flag = false;
                }

            }catch (Exception e){
                logger.error("商品上架失败",e.getMessage());
                flag = false;
            }
        }



    }
}
