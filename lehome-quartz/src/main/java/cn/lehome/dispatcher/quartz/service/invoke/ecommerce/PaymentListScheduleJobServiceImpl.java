package cn.lehome.dispatcher.quartz.service.invoke.ecommerce;

import cn.lehome.base.api.business.ec.bean.ecommerce.order.OrderIndex;
import cn.lehome.base.api.business.ec.bean.ecommerce.pay.PayRecord;
import cn.lehome.base.api.business.ec.bean.ecommerce.pay.QPayRecord;
import cn.lehome.base.api.business.ec.bean.ecommerce.store.Store;
import cn.lehome.base.api.business.ec.enums.PayStatus;
import cn.lehome.base.api.business.ec.service.ecommerce.order.OrderApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.order.OrderIndexApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.pay.PayApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.pay.PayRecordApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.pay.PayRecordIndexApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.store.StoreApiService;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.base.api.common.pay.bean.CommonResponse;
import cn.lehome.base.api.common.pay.bean.trade.QueryOrderResponse;
import cn.lehome.base.api.common.pay.service.alipay.merchant.MerchantAlipayApiService;
import cn.lehome.base.api.common.pay.service.wxpay.merchant.WXPayMerchantApiService;
import cn.lehome.bean.business.ec.enums.ecommerce.order.OrderStatus;
import cn.lehome.bean.business.ec.enums.ecommerce.order.OrderType;
import cn.lehome.bean.business.ec.enums.ecommerce.pay.BillType;
import cn.lehome.bean.business.ec.enums.ecommerce.pay.PayType;
import cn.lehome.bean.business.ec.enums.ecommerce.pay.TransactionProgress;
import cn.lehome.bean.business.ec.enums.ecommerce.store.StoreStatus;
import cn.lehome.bean.business.ec.enums.ecommerce.store.StoreType;
import cn.lehome.bean.pay.enums.PaySource;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.compoment.loader.LoaderServiceComponent;
import cn.lehome.framework.base.api.core.compoment.request.ApiPageRequestHelper;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;

@Service("paymentListScheduleJobService")
public class PaymentListScheduleJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private PayRecordApiService payRecordApiService;

    @Autowired
    private PayRecordIndexApiService payRecordIndexApiService;

    @Autowired
    private PayApiService payApiService;

    @Autowired
    private OrderIndexApiService orderIndexApiService;

    @Autowired
    private StoreApiService storeApiService;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Autowired
    private LoaderServiceComponent loaderServiceComponent;

//    @Autowired
//    private ActionLogRequest actionLogRequest;

    @Autowired
    private OrderApiService orderApiService;

    @Autowired
    protected MerchantAlipayApiService merchantAlipayApiService;

    @Autowired
    protected WXPayMerchantApiService wxPayMerchantApiService;


    @Value("${wx.app.appId}")
    protected String wxAppAppId;

    @Value("${wx.small.appId}")
    protected String wxSmallAppId;

    @Override
    public void doInvoke(Map<String, String> params) {

        logger.error("进入支付单处理定时任务");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, -3);
        Date time = calendar.getTime();
        List<PayRecord> payRecordIndexList = ApiPageRequestHelper.request(ApiRequest.newInstance().filterEqual(QPayRecord.billType, BillType.NORMAL).filterEqual(QPayRecord.status, TransactionProgress.CREATE_TRADING_SHEET).filterLessEqual(QPayRecord.createdTime, time), ApiRequestPage.newInstance().paging(0, 100), payRecordApiService::findAll);
        logger.error("支付单集合共" + payRecordIndexList.size() + "条");
        if (CollectionUtils.isEmpty(payRecordIndexList)) {
            return;
        }
        List<OrderIndex> successList = new ArrayList<>();

        for (PayRecord payRecordIndex : payRecordIndexList) {
            logger.error("支付单" + payRecordIndex.getId());
            try {
                QueryOrderResponse queryOrderResponse = null;
                if (payRecordIndex.getPayType().equals(PayType.WECHAT)) {
                    if (payRecordIndex.getClientId().equals("sqbj-ecommerce-small")) {
                        queryOrderResponse = wxPayMerchantApiService.queryOrder(payRecordIndex.getId(), PaySource.SQBJ, wxSmallAppId);
                    } else {
                        queryOrderResponse = wxPayMerchantApiService.queryOrder(payRecordIndex.getId(), PaySource.SQBJ, wxAppAppId);
                    }
                } else {
                    queryOrderResponse = merchantAlipayApiService.queryOrder(payRecordIndex.getId(), PaySource.SQBJ);
                }
                if (!queryOrderResponse.isResStatus()) {
                    logger.error("查询账单错误 ：" + JSON.toJSONString(queryOrderResponse));
                    return;
                }

                PayStatus payStatus = PayStatus.valueOf(queryOrderResponse.getPayStatus().toString());
                TransactionProgress transactionProgress = null;
                switch (payStatus) {
                    case SUCCESS:
                        this.paySuccess(payRecordIndex, successList);
                        transactionProgress = TransactionProgress.PAY_SUCCESS;
                        break;
                    case REFUND:
                        break;
                    case NOTPAY:
                        this.payNopay(payRecordIndex);
                        break;
                    case CLOSED:
                        this.payClose(payRecordIndex);
                        break;
                    case REVOKED:
                        break;
                    case USERPAYING:
                        break;
                    case PAYERROR:
                        this.payFailed(payRecordIndex);
                        break;
                    case REFUND_SUCCESS:
                        break;
                    case REFUNDCLOSE:
                        break;
                    case PROCESSING:
                        break;
                    case CHANGE:
                        break;
                }
                if (transactionProgress != null) {

                }
            } catch (Exception e) {
                logger.error("检查支付单状态错误:", e.getMessage(), e);
            }
        }

//        if (!CollectionUtils.isEmpty(successList)) {
//            ActionLog.Builder builder = ActionLog.newBuilder();
//            for (OrderIndex orderIndex : successList) {
//                builder.addActionLogBean(AppActionLog.newBuilder(BusinessActionKey.ORDER_STATUS_CHANGE, orderIndex.getUserOpenId(), orderIndex.getClientId())
//                        .addMap("orderId", orderIndex.getId())
//                        .addMap("prevOrderStatus", OrderStatus.OBLIGATION)
//                        .addMap("orderStatus", OrderStatus.WAIT_SHIPMENTS).build()
//                );
//            }
//            builder.send(actionLogRequest);
//        }
    }

    private void payFailed(PayRecord payRecordIndex) {
        if (!payRecordApiService.updateStatus(payRecordIndex.getId(), TransactionProgress.PAY_FAIL)) {
            logger.error("修改支付单状态失败, id = " + payRecordIndex.getId());
        }
    }

    private void payClose(PayRecord payRecordIndex) {
        if (!payRecordApiService.updateStatus(payRecordIndex.getId(), TransactionProgress.PAY_CANCEL)) {
            logger.error("修改支付单状态失败, id = " + payRecordIndex.getId());
        }
    }

    private void payNopay(PayRecord payRecordIndex) {
        CommonResponse commonResponse = null;
        if (payRecordIndex.getPayType().equals(PayType.WECHAT)) {
            if (payRecordIndex.getClientId().equals("sqbj-ecommerce-small")) {
                commonResponse = wxPayMerchantApiService.closeOrder(payRecordIndex.getId(), PaySource.SQBJ, wxSmallAppId);
            } else {
                commonResponse = wxPayMerchantApiService.closeOrder(payRecordIndex.getId(), PaySource.SQBJ, wxAppAppId);
            }
        } else {
            commonResponse = merchantAlipayApiService.closeOrder(payRecordIndex.getId(), PaySource.SQBJ);
        }
        if (commonResponse.isResStatus()) {
            if (!payRecordApiService.updateStatus(payRecordIndex.getId(), TransactionProgress.PAY_CANCEL)) {
                logger.error("修改支付单状态失败, id = " + payRecordIndex.getId());
            }
        }
    }

    private void paySuccess(PayRecord payRecordIndex, List<OrderIndex> updateList) {
        boolean result = payRecordApiService.updateStatus(payRecordIndex.getId(), TransactionProgress.PAY_SUCCESS);
        if (!result) {
            logger.error("修改支付单状态失败");
            return;
        }

        List<String> strings = JSON.parseArray(payRecordIndex.getOrderNo(), String.class);
        if (CollectionUtils.isEmpty(strings)) {
            logger.error("订单单号为空, payRecordId = " + payRecordIndex.getId());
            return;
        }

        List<OrderIndex> orderIndexList = orderIndexApiService.findAllByTotalOrderNo(strings);
        if (CollectionUtils.isEmpty(orderIndexList)) {
            logger.error("订单未找到, payRecordId = " + payRecordIndex.getId());
            return;
        }

        loaderServiceComponent.loadAllBatch(orderIndexList, OrderIndex.class);

        orderIndexList.forEach(orderIndex -> orderApiService.updateStatusForSys(orderIndex.getId(), OrderStatus.WAIT_SHIPMENTS));
        
        eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.PAYRECORD_STATUS_CHANGE_EVENT, new HashMap.SimpleEntry<>(payRecordIndex.getId(), TransactionProgress.PAY_SUCCESS)));
        updateList.addAll(orderIndexList);

        //检查是否为开店礼包订单
        if (orderIndexList.get(0).getOrderType().equals(OrderType.GIFT)) {
            Store store = storeApiService.findByUserAccountIdAndStoreTypeAndStoreStatus(orderIndexList.get(0).getUserAccountId(), StoreType.PERSONAL, StoreStatus.NEW);
            if (store != null) {
                storeApiService.updateStatus(store.getId(), StoreStatus.WAIT_CONFIRM);
            }
        }
    }
}
