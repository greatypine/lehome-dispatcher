package cn.lehome.dispatcher.quartz.service.invoke.ecommerce;

import cn.lehome.base.api.business.bean.ecommerce.order.*;
import cn.lehome.base.api.business.service.ecommerce.goods.GoodsSpuIndexApiService;
import cn.lehome.base.api.business.service.ecommerce.order.GoodsGroupbuyApiService;
import cn.lehome.base.api.business.service.ecommerce.order.GoodsGroupbuyIndexApiService;
import cn.lehome.base.api.business.service.ecommerce.order.OrderApiService;
import cn.lehome.base.api.business.service.ecommerce.order.OrderIndexApiService;
import cn.lehome.base.api.tool.compoment.jms.EventBusComponent;
import cn.lehome.base.api.tool.service.idgenerator.RedisIdGeneratorApiService;
import cn.lehome.bean.business.enums.ecommerce.order.*;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.compoment.loader.LoaderServiceComponent;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;

@Service("groupPurchaseBeginsScheduleJobService")
public class GroupPurchaseBeginsScheduleJobServiceImpl extends AbstractInvokeServiceImpl{

    @Autowired
    private GoodsGroupbuyApiService goodsGroupbuyApiService;

    @Autowired
    private GoodsGroupbuyIndexApiService goodsGroupbuyIndexApiService;

    @Override
    public void doInvoke(Map<String, String> params) {

        logger.error("进入团购开始定时任务");
        List<GoodsGroupbuyIndex> goodsGroupbuyIndexList = goodsGroupbuyIndexApiService.findAll(ApiRequest.newInstance().filterLike(QGoodsGroupbuyIndex.status, GroupbuyStatus.NOT_PROCEEDED).filterLessEqual(QGoodsGroupbuyIndex.startTime, new Date().getTime()));
        if (!CollectionUtils.isEmpty(goodsGroupbuyIndexList)){
            goodsGroupbuyIndexList.stream().forEach(goodsGroupbuyIndex -> {
                goodsGroupbuyApiService.updateStatus(GroupbuyStatus.PROCEED,goodsGroupbuyIndex.getId());
            });
        }
    }

}
