package cn.lehome.dispatcher.quartz.service.invoke.ecommerce;

import cn.lehome.base.api.business.ec.bean.ecommerce.order.GoodsGroupbuyIndex;
import cn.lehome.base.api.business.ec.bean.ecommerce.order.QGoodsGroupbuyIndex;
import cn.lehome.base.api.business.ec.service.ecommerce.order.GoodsGroupbuyApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.order.GoodsGroupbuyIndexApiService;
import cn.lehome.bean.business.ec.enums.ecommerce.order.GroupbuyStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;

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
