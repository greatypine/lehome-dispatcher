package cn.lehome.dispatcher.queue.listener.goods;

import cn.lehome.base.api.business.bean.ecommerce.goods.GoodsSpuIndex;
import cn.lehome.base.api.business.bean.ecommerce.goods.QGoodsSpuIndex;
import cn.lehome.base.api.business.service.ecommerce.goods.GoodsCatalogIndexApiService;
import cn.lehome.base.api.business.service.ecommerce.goods.GoodsSpuIndexApiService;
import cn.lehome.dispatcher.queue.bean.CatalogMqBean;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.compoment.request.ApiPageRequestHelper;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;

public class GoodsCatalogChangeListener extends AbstractJobListener {

    @Autowired
    private GoodsCatalogIndexApiService goodsCatalogIndexApiService;

    @Autowired
    private GoodsSpuIndexApiService goodsSpuIndexApiService;


    @Override
    public void execute(IEventMessage eventMessage) throws Exception {

        SimpleEventMessage<CatalogMqBean> simpleEventMessage = (SimpleEventMessage<CatalogMqBean>) eventMessage;
        CatalogMqBean messageData = simpleEventMessage.getData();
        logger.info("修改分类信息接收到参数：catalogId={}, catalogName={}, parentCataLogId={}", messageData.getCatalogId(), messageData.getCatalogName(), messageData.getParentCatalogId());

        ApiRequest apiRequest = ApiRequest.newInstance();
        if (messageData.getParentCatalogId() != null && messageData.getParentCatalogId() != 0L) {
            apiRequest.filterEqual(QGoodsSpuIndex.secondGoodsCatalogId, messageData.getCatalogId());
        } else {
            apiRequest.filterEqual(QGoodsSpuIndex.firstGoodsCatalogId, messageData.getCatalogId());
        }
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance().paging(0, 20);
        List<GoodsSpuIndex> spuIndexList = ApiPageRequestHelper.request(apiRequest, apiRequestPage, goodsSpuIndexApiService::findAll);
        if (CollectionUtils.isEmpty(spuIndexList)) {
            return;
        }

        spuIndexList.parallelStream().forEach(spu -> {
            if (messageData.getParentCatalogId() != null && messageData.getParentCatalogId() != 0L) {
                spu.setSecondGoodsCatalogName(messageData.getCatalogName());
            } else {
                spu.setFirstGoodsCatalogName(messageData.getCatalogName());
            }
            goodsSpuIndexApiService.saveOrUpdate(spu);
        });


    }

    @Override
    public String getConsumerId() {
        return "goods_catalog_change";
    }
}
