package cn.lehome.dispatcher.quartz.service.invoke.ecommerce;

import cn.lehome.base.api.business.ec.bean.ecommerce.goods.GoodsSpuIndex;
import cn.lehome.base.api.business.ec.bean.ecommerce.order.*;
import cn.lehome.base.api.business.ec.bean.ecommerce.pay.PayRecord;
import cn.lehome.base.api.business.ec.service.ecommerce.goods.GoodsSpuIndexApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.order.GoodsGroupbuyApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.order.OrderApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.order.OrderBackApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.order.OrderIndexApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.pay.PayRecordApiService;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.base.api.common.pay.bean.alipay.AliRefundRequest;
import cn.lehome.base.api.common.pay.bean.trade.RefundResponse;
import cn.lehome.base.api.common.pay.bean.wxpay.WXRefundRequest;
import cn.lehome.base.api.common.pay.service.alipay.merchant.MerchantAlipayApiService;
import cn.lehome.base.api.common.pay.service.wxpay.merchant.WXPayMerchantApiService;
import cn.lehome.base.api.common.service.idgenerator.RedisIdGeneratorApiService;
import cn.lehome.bean.business.ec.enums.ecommerce.goods.SaleStatus;
import cn.lehome.bean.business.ec.enums.ecommerce.order.BackReason;
import cn.lehome.bean.business.ec.enums.ecommerce.order.GroupbuyStatus;
import cn.lehome.bean.business.ec.enums.ecommerce.order.OrderBackStatus;
import cn.lehome.bean.business.ec.enums.ecommerce.order.OrderStatus;
import cn.lehome.bean.business.ec.enums.ecommerce.pay.PayType;
import cn.lehome.bean.business.ec.enums.ecommerce.pay.TransactionProgress;
import cn.lehome.bean.pay.enums.PaySource;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.compoment.loader.LoaderServiceComponent;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.exception.RestfulApiException;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;

@Service("groupPurchaseOrderScheduleJobService")
public class GroupPurchaseOrderScheduleJobServiceImpl extends AbstractInvokeServiceImpl{

    @Autowired
    private GoodsGroupbuyApiService goodsGroupbuyApiService;

    @Autowired
    private GoodsSpuIndexApiService goodsSpuIndexApiService;

    @Autowired
    private OrderApiService orderApiService;

    @Autowired
    private OrderIndexApiService orderIndexApiService;

    @Autowired
    private LoaderServiceComponent loaderServiceComponent;

    @Autowired
    private EventBusComponent eventBusComponent;

    private static final String ORDER_NO = "business.orderNo:";

    @Autowired
    private RedisIdGeneratorApiService redisIdGeneratorApiService;

//    @Autowired
//    private ActionLogRequest actionLogRequest;

    @Autowired
    private OrderBackApiService orderBackApiService;

    @Autowired
    private PayRecordApiService payRecordApiService;

    @Autowired
    private MerchantAlipayApiService merchantAlipayApiService;

    @Autowired
    private WXPayMerchantApiService wxPayMerchantApiService;

    @Value("${wx.refund.notify}")
    protected String wxRefundNotify;

    @Value("${wx.app.appId}")
    protected String wxAppAppId;

    @Value("${wx.small.appId}")
    protected String wxSmallAppId;

    @Override
    public void doInvoke(Map<String, String> params) {
        logger.error("进入团购订单处理定时任务 start");
        List<GoodsGroupbuy> goodsGroupbuyIndexList = goodsGroupbuyApiService.findAll(ApiRequest.newInstance().filterEqual(QGoodsGroupbuyIndex.status, GroupbuyStatus.PROCEED).filterLessEqual(QGoodsGroupbuyIndex.endTime, new Date()));
        logger.error("团购列表：" + JSON.toJSONString(goodsGroupbuyIndexList));
        if (!CollectionUtils.isEmpty(goodsGroupbuyIndexList)) {
            for (GoodsGroupbuy goodsGroupbuy : goodsGroupbuyIndexList) {
                try {
                    if (checkOpenGroupBuy(goodsGroupbuy)) {
                        goodsGroupbuyApiService.updateStatus(GroupbuyStatus.SUCCESS, goodsGroupbuy.getId());
                    } else {
                        goodsGroupbuyApiService.updateStatus(GroupbuyStatus.FAIL, goodsGroupbuy.getId());
                        //修改已经抚玩款的订单
                        List<OrderIndex> orderIndexList = orderIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QOrderIndex.storeId, goodsGroupbuy.getStoreId()).filterEqual(QOrderIndex.groupbuyGoodsId, goodsGroupbuy.getId()).filterLike(QOrderIndex.status,OrderStatus.WAIT_SHIPMENTS));
                        if (!CollectionUtils.isEmpty(orderIndexList)) {
                            orderIndexList.forEach(this::cancelOrder);
                        }
                        //修改未付款的订单
                        orderIndexList = orderIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QOrderIndex.storeId, goodsGroupbuy.getStoreId()).filterEqual(QOrderIndex.groupbuyGoodsId, goodsGroupbuy.getId()).filterLike(QOrderIndex.status,OrderStatus.OBLIGATION));
                        if (!CollectionUtils.isEmpty(orderIndexList)) {
                            orderIndexList.forEach(this::cancelUnpayOrder);
                        }


                    }
                } catch (Exception e) {
                    logger.error("处理失败:", e.getMessage(), e);
                }
            }
        }
        logger.error("进入团购订单处理定时任务 end");
    }

    private boolean checkOpenGroupBuy(GoodsGroupbuy goodsGroupbuy) {
        GoodsSpuIndex goodsSpuIndex = goodsSpuIndexApiService.get(goodsGroupbuy.getGoodsId().toString());
        if (goodsGroupbuy == null) {
            logger.error("商品信息未找到, groupBuyId = " + goodsGroupbuy.getId());
            return false;
        }
        if (!goodsSpuIndex.getSaleStatus().equals(SaleStatus.SHELVES)) {
            logger.error("商品不是上架状态, goodsId = " + goodsSpuIndex.getId());
            return false;
        }
        List<OrderIndex> orderIndexList = orderIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QOrderIndex.storeId, goodsGroupbuy.getStoreId()).filterEqual(QOrderIndex.groupbuyGoodsId, goodsGroupbuy.getId()).filterLike(QOrderIndex.status,OrderStatus.WAIT_SHIPMENTS));
        if (orderIndexList == null || orderIndexList.size() == 0) {
            logger.error("没有购买成功订单, groupBuyId = " + goodsGroupbuy.getId());
            return false;
        }
        loaderServiceComponent.loadAllBatch(orderIndexList, OrderIndex.class);
        List<OrderDetailIndex> orderDetailIndexList = new ArrayList<>();
        orderIndexList.stream().forEach(orderIndex -> orderDetailIndexList.addAll(orderIndex.getOrderDetailIndexList()));
        int sum = orderDetailIndexList.stream().mapToInt(OrderDetailIndex::getGoodsCount).sum();
        if (sum < goodsGroupbuy.getMinGroupCount()) {
            logger.error("开团不成功, groupBuyId = " + goodsGroupbuy.getId());
            return false;
        }
        return true;
    }

    private void cancelUnpayOrder(OrderIndex orderIndex) {
        boolean result = orderApiService.updateStatusForSys(orderIndex.getId(), OrderStatus.CANCEL);
        if (!result) {
            throw new RestfulApiException("取消订单失败");
        }
//        AppActionLog.newBuilder(BusinessActionKey.ORDER_STATUS_CHANGE, "", "cms")
//                .addMap("orderId", orderIndex.getId())
//                .addMap("prevOrderStatus", OrderStatus.OBLIGATION)
//                .addMap("orderStatus", OrderStatus.CANCEL)
//                //.addMap 添加多个属性信息
//                .send(actionLogRequest);
    }

    private void cancelOrder(OrderIndex orderIndex) {
        boolean result = orderApiService.updateStatusForSys(orderIndex.getId(), OrderStatus.CANCEL_AND_WAIT_AUDIT);

        if (!result) {
            logger.error("修改订单壮体啊失败, orderId = " + orderIndex.getId());
            return;
        }

//        AppActionLog.newBuilder(BusinessActionKey.ORDER_STATUS_CHANGE, "", "cms")
//                .addMap("orderId", orderIndex.getId())
//                .addMap("prevOrderStatus", OrderStatus.WAIT_RECEIVE)
//                .addMap("orderStatus", OrderStatus.CANCEL_AND_WAIT_AUDIT)
//                //.addMap 添加多个属性信息
//                .send(actionLogRequest);

        result = orderApiService.updateStatusForSys(orderIndex.getId(), OrderStatus.CANCEL);

        if (!result) {
            logger.error("修改订单壮体啊失败, orderId = " + orderIndex.getId());
            return;
        }

//        AppActionLog.newBuilder(BusinessActionKey.ORDER_STATUS_CHANGE, "", "cms")
//                .addMap("orderId", orderIndex.getId())
//                .addMap("prevOrderStatus", OrderStatus.CANCEL_AND_WAIT_AUDIT)
//                .addMap("orderStatus", OrderStatus.CANCEL)
//                //.addMap 添加多个属性信息
//                .send(actionLogRequest);

        List<OrderBack> orderBackList = orderBackApiService.findByOrderId(orderIndex.getId());
        if (orderBackList == null) {
            logger.error("未找到退款订单, orderId = " + orderIndex.getId());
            return;
        }
        OrderBack orderBack = null;
        for (OrderBack back : orderBackList) {
            if (back.getBackReason().equals(BackReason.ORDER_CANCEL) && back.getStatus().equals(OrderBackStatus.AFFIRM_REIMBURSE)) {
                orderBack = back;
            }
        }
        if (orderBack == null) {
            logger.error("未找到退款订单, orderId = " + orderIndex.getId());
            return;
        }
        if (StringUtils.isEmpty(orderBack.getPayRecordId())) {
            logger.error("退款订单没有支付单ID, orderId = " + orderIndex.getId());
            return;
        }

        result = payRecordApiService.updateStatus(orderBack.getPayRecordId(), TransactionProgress.PAY_DURING);

        if (!result) {
            logger.error("修改支付单状态失败, payRecordId = " + orderBack.getPayRecordId());
            return;
        }

        PayRecord payRecord =  payRecordApiService.findOne(orderBack.getPayRecordId());
        if(payRecord != null) {
            boolean isResult = true;
            if (payRecord.getPayType().equals(PayType.ALIPAY)) {
                AliRefundRequest aliRefundRequest = new AliRefundRequest();
                aliRefundRequest.setOrderId(payRecord.getId());
                aliRefundRequest.setRefundOrderId(payRecord.getId());
                aliRefundRequest.setRefundFee(payRecord.getPayMoney().intValue());
                aliRefundRequest.setTotalFee(payRecord.getPayMoney().intValue());
                aliRefundRequest.setPaySource(PaySource.SQBJ);
                RefundResponse refundResponse = merchantAlipayApiService.refundOrder(aliRefundRequest);
                if (!refundResponse.isResStatus()) {
                    isResult = false;
                } else {
                    Map.Entry<String, TransactionProgress> entry = new HashMap.SimpleEntry<>(payRecord.getId(), TransactionProgress.PAY_SUCCESS);
                    eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.PAYRECORD_STATUS_CHANGE_EVENT, entry));
                }
            } else {
                WXRefundRequest wxRefundRequest = new WXRefundRequest();
                if (payRecord.getClientId().equals("sqbj-ecommerce-small")) {
                    wxRefundRequest.setAppId(wxSmallAppId);
                } else {
                    wxRefundRequest.setAppId(wxAppAppId);
                }
                wxRefundRequest.setNotifyUrl(wxRefundNotify);
                wxRefundRequest.setOrderId(payRecord.getId());
                wxRefundRequest.setRefundOrderId(payRecord.getId());
                wxRefundRequest.setRefundFee(payRecord.getPayMoney().intValue());
                wxRefundRequest.setTotalFee(payRecord.getPayMoney().intValue());
                wxRefundRequest.setPaySource(PaySource.SQBJ);
                RefundResponse refundResponse = wxPayMerchantApiService.refundOrder(wxRefundRequest);
                if (!refundResponse.isResStatus()) {
                    isResult = false;
                }
            }

            logger.info("团购自动退款, payRecordId = {}, result = {}", orderBack.getPayRecordId(), isResult);
            payRecordApiService.forcedRefund(orderBack.getPayRecordId(), isResult);

            loaderServiceComponent.load(orderIndex, QOrderIndex.orderDetailIndexList);

            if (orderIndex.getOrderDetailIndexList() == null || orderIndex.getOrderDetailIndexList().size() == 0) {
                logger.error("团购订单自订单未找到, orderId = " + orderIndex.getId());
                return;
            }
        }
    }


}
