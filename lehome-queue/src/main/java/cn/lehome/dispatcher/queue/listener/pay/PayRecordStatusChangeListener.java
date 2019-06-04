package cn.lehome.dispatcher.queue.listener.pay;

import cn.lehome.base.api.business.ec.bean.ecommerce.order.*;
import cn.lehome.base.api.business.ec.bean.ecommerce.pay.PayRecordIndex;
import cn.lehome.base.api.business.ec.bean.ecommerce.store.BrokerageRecord;
import cn.lehome.base.api.business.ec.bean.ecommerce.store.BrokerageRecordIndex;
import cn.lehome.base.api.business.ec.bean.ecommerce.store.QBrokerageRecord;
import cn.lehome.base.api.business.ec.bean.ecommerce.store.Store;
import cn.lehome.base.api.business.ec.service.ecommerce.order.*;
import cn.lehome.base.api.business.ec.service.ecommerce.pay.PayRecordIndexApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.store.BrokerageRecordApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.store.BrokerageRecordIndexApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.store.StoreApiService;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.custom.oauth2.bean.user.UserAccountIndex;
import cn.lehome.base.api.common.custom.oauth2.service.user.UserAccountIndexApiService;
import cn.lehome.bean.business.ec.constants.BusinessActionKey;
import cn.lehome.bean.business.ec.enums.ecommerce.order.OrderBackStatus;
import cn.lehome.bean.business.ec.enums.ecommerce.order.OrderStatus;
import cn.lehome.bean.business.ec.enums.ecommerce.order.OrderType;
import cn.lehome.bean.business.ec.enums.ecommerce.pay.BillType;
import cn.lehome.bean.business.ec.enums.ecommerce.pay.TransactionProgress;
import cn.lehome.bean.business.ec.enums.ecommerce.store.BrokerageSettleStatus;
import cn.lehome.bean.business.ec.enums.ecommerce.store.StoreStatus;
import cn.lehome.bean.business.ec.enums.ecommerce.store.StoreType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.actionlog.core.ActionLogRequest;
import cn.lehome.framework.actionlog.core.bean.ActionLog;
import cn.lehome.framework.actionlog.core.bean.AppActionLog;
import cn.lehome.framework.base.api.core.compoment.loader.LoaderServiceComponent;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The type Pay record status change listener.
 *
 * @author zhuzz
 * @time 2018 /12/19 11:36:16
 */
public class PayRecordStatusChangeListener extends AbstractJobListener {


    @Autowired
    private PayRecordIndexApiService payRecordIndexApiService;

    @Autowired
    private OrderIndexApiService orderIndexApiService;


    @Autowired
    private LoaderServiceComponent loaderServiceComponent;

    @Autowired
    private ActionLogRequest actionLogRequest;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Autowired
    private StoreApiService storeApiService;

    @Autowired
    private UserAccountIndexApiService userAccountIndexApiService;

    @Autowired
    private OrderApiService orderApiService;

    @Autowired
    private OrderBackIndexApiService orderBackIndexApiService;

    @Autowired
    private OrderBackApiService orderBackApiService;

    @Autowired
    private BrokerageRecordApiService brokerageRecordApiService;

    @Autowired
    private BrokerageRecordIndexApiService brokerageRecordIndexApiService;

    @Autowired
    private OrderDetailApiService orderDetailApiService;

    @Autowired
    private OrderDetailIndexApiService orderDetailIndexApiService;


    @Override
    public void execute(IEventMessage eventMessage) throws Exception {

        SimpleEventMessage<Map.Entry<String, TransactionProgress>> simpleEventMessage = (SimpleEventMessage<Map.Entry<String, TransactionProgress>>) eventMessage;

        Map.Entry<String, TransactionProgress> data = simpleEventMessage.getData();

        logger.error("回调处理支付单！ payrecord={}", JSON.toJSONString(data));

        PayRecordIndex payRecordIndex = payRecordIndexApiService.findOne(data.getKey());
        if (Objects.isNull(payRecordIndex)) {
            logger.error("支付单不存在！ payrecord={}", JSON.toJSONString(data));
            return;
        }

        //当前消息只处理付款成功回调相关的业务处理  其他状态不做处理(相关定时任务中做处理)
        //保证回调接口必要更改的执行 其他关联业务此处处理
        switch (data.getValue()) {
            case CREATE_TRADING_SHEET:
                break;
            case PAY_SUCCESS:

                //只有订单付款与退款单有回调
                switch (payRecordIndex.getBillType()) {
                    case NORMAL:
                        this.normalOrderPaySuccess(payRecordIndex);
                        break;
                    case RETURN:
                        this.orderBackPaySuccess(payRecordIndex);
                        break;
                    case WITHDRAW_DEPOSIT:
                        break;
                    case CANCEL_ORDER:
                        this.orderBackPaySuccess(payRecordIndex);
                        break;
                }
                break;
            case PAY_FAIL:
                break;
            case PAY_CANCEL:
                break;
            case PAY_DURING:
                break;
            case PAY_ERROR:
                break;
        }


    }

    private void orderBackPaySuccess(PayRecordIndex payRecordIndex) {

        OrderBackIndex orderBackIndex = orderBackIndexApiService.findByPayRecordId(payRecordIndex.getId());
        OrderBackStatus prevStatus = orderBackIndex.getStatus();

        orderBackApiService.updateStatusForSys(orderBackIndex.getId(), OrderBackStatus.REIMBURSE_SUCCESS);

        if (payRecordIndex.getBillType().equals(BillType.CANCEL_ORDER)) {
            Long orderId = orderBackIndex.getOrderId();
            OrderIndex orderIndex = orderIndexApiService.findOne(orderId);
            loaderServiceComponent.load(orderIndex, QOrderIndex.orderDetailIndexList);
            orderIndex.getOrderDetailIndexList().forEach(this::brokerage_cancelOrder);
        } else if (payRecordIndex.getBillType().equals(BillType.RETURN)) {
            this.brokerage_refund(orderBackIndex);
        }
        AppActionLog.newBuilder(BusinessActionKey.ORDER_BACK_STATUS_CHANGE).addMap("orderBackId", orderBackIndex.getId()).addMap("prevOrderBackStatus", prevStatus).addMap("orderBackStatus", orderBackIndex.getStatus()).send(actionLogRequest);

    }

    private void normalOrderPaySuccess(PayRecordIndex payRecordIndex) {

        List<String> strings = JSON.parseArray(payRecordIndex.getOrderNo(), String.class);
        List<OrderIndex> orderIndexList = orderIndexApiService.findAllByTotalOrderNo(strings);
        loaderServiceComponent.loadAllBatch(orderIndexList, OrderIndex.class);
        ActionLog.Builder builder = ActionLog.newBuilder();

        orderIndexList.forEach(p -> {
            if(orderApiService.updateStatusForSys(p.getId(), OrderStatus.WAIT_SHIPMENTS)){
                builder.addActionLogBean(AppActionLog.newBuilder(BusinessActionKey.ORDER_STATUS_CHANGE, p.getUserOpenId(), p.getClientId())
                        .addMap("orderId", p.getId())
                        .addMap("prevOrderStatus", OrderStatus.OBLIGATION)
                        .addMap("orderStatus", OrderStatus.WAIT_SHIPMENTS).build()
                );
            }
        });

        //判断是否是开店礼包
        if (orderIndexList.get(0).getOrderType().equals(OrderType.GIFT)) {
            Long userAccountId = orderIndexList.get(0).getUserAccountId();
            Store store = storeApiService.findByUserAccountId(userAccountId);
            if (store.getStoreStatus().equals(StoreStatus.NEW)) {
                storeApiService.updateStatus(store.getId(), StoreStatus.WAIT_CONFIRM);
                logger.info("开店礼包支付成功修改该用户对应的店铺状态，userAccountId:{}，storeId:{}", userAccountId, store.getId());
            }
        }

        orderIndexList.forEach(p -> p.getOrderDetailIndexList().forEach(this::brokerage_payComplete));

        builder.send(actionLogRequest);
    }


    /**
     * 订单支付完成
     *
     * @param orderDetailIndex the detail
     *
     * @author zhuzz
     * @time 2018 /11/26 10:31:34
     */
    public void brokerage_payComplete(OrderDetailIndex orderDetailIndex) {


        logger.error("pay complete orderDetailId={}", orderDetailIndex.getId());

        List<BrokerageRecord> records = brokerageRecordApiService.findAll(ApiRequest.newInstance().filterEqual(QBrokerageRecord.orderId, orderDetailIndex.getId()));
        if (!CollectionUtils.isEmpty(records)) {
            logger.error("pay complete orderDetailId={} record repeat! ", orderDetailIndex.getId());
            return;
        }

        Order mainOrder = orderApiService.findOne(orderDetailIndex.getOrderId());

        //判断是否是社区半径店铺类型
        Store store = storeApiService.findOne(mainOrder.getStoreId());
        if (StoreType.COMPANY.equals(store.getStoreType())) {
            return;
        }

        //当前订单所得佣金
        BigDecimal broker = this.getBroker(orderDetailIndex.getTotalMoney(), orderDetailIndex.getBrokerageRate()).setScale(0, BigDecimal.ROUND_DOWN);

        if (broker.intValue() < 1) {
            logger.error("pay complete orderDetailId={} brokerage is zore ! ", orderDetailIndex.getId());
            return;
        }


        BrokerageRecord record = new BrokerageRecord();

        record.setAfterDeductBrokerage(BigDecimal.ZERO);
        record.setDeductBrokerage(BigDecimal.ZERO);
        record.setBrokerage(broker);
        record.setStoreId(mainOrder.getStoreId());
        record.setOrderId(orderDetailIndex.getId());
        record.setStatus(BrokerageSettleStatus.NOT_SETTLE);

        UserAccountIndex userAccount = userAccountIndexApiService.getUserAccount(mainOrder.getUserAccountId().toString());

        //冗余字段
        record.setGroupBuy(Objects.nonNull(mainOrder.getGroupbuyGoodsId()) && mainOrder.getGroupbuyGoodsId() > 0);
        record.setMoney(orderDetailIndex.getTotalMoney());
        record.setUserName(userAccount.getNickName());
        record.setUserPhone(userAccount.getPhone());
        record.setOrderNo(orderDetailIndex.getOrderNo());
        record.setOrderTime(mainOrder.getCreatedTime());
        record.setGoodsSkuName(orderDetailIndex.getGoodsSkuName());
        record.setGoodsName(orderDetailIndex.getGoodsSpuName());
        record.setGoodsImageUrl(orderDetailIndex.getGoodsImageUrl());
        record.setStoreUserId(mainOrder.getStoreUserId());

        brokerageRecordApiService.orderPayComplete(record, mainOrder.getStoreId(), orderDetailIndex.getOrderNo());

        BigDecimal tax = BigDecimal.ONE.add(orderDetailIndex.getBrokerageRate().divide(BigDecimal.valueOf(100), 6, RoundingMode.HALF_UP));
        BigDecimal orderMoneyNoTax = orderDetailIndex.getTotalMoney().divide(tax, 6, RoundingMode.HALF_UP);
        BigDecimal goodsPurchasePriceNoTax = orderDetailIndex.getGoodsPurchasePrice().divide(tax, 6, RoundingMode.HALF_UP);
        BigDecimal serviceMoney = orderDetailIndex.getTotalMoney().multiply(BigDecimal.valueOf(0.006));

        orderDetailIndex.setOrderMoneyNoTax(orderMoneyNoTax.setScale(6, RoundingMode.HALF_UP));
        orderDetailIndex.setGoodsPurchasePriceNoTax(goodsPurchasePriceNoTax.setScale(6, RoundingMode.HALF_UP));
        orderDetailIndex.setServiceMoney(serviceMoney.setScale(6, RoundingMode.HALF_UP));

        orderDetailIndexApiService.update(orderDetailIndex);


        AppActionLog.newBuilder(BusinessActionKey.BROKERAGE_RECORD_STATUS_CHANGE).addMap("brokerageRecordId", record.getId()).addMap("status", record.getStatus()).send(actionLogRequest);
        logger.error("pay complete  orderDetailId={}, BrokerageRecord={} ", orderDetailIndex.getId(), broker);
    }

    public void brokerage_refund(OrderBackIndex lastOrderBack) {

        Long orderDetailId = lastOrderBack.getOrderDetailId();

        OrderDetail detail = orderDetailApiService.findOne(orderDetailId);

        logger.error("orderRefund orderDetailId={}", orderDetailId);
        BrokerageRecordIndex recordIndex = brokerageRecordIndexApiService.getByOrderId(orderDetailId);

        if (Objects.isNull(recordIndex)) {
            logger.error("orderRefund orderDetailId={} brokerage is null", orderDetailId);
            return;
        }

        Order mainOrder = orderApiService.findOne(detail.getOrderId());

//        判断是否是社区半径店铺类型
        Store store = storeApiService.findOne(mainOrder.getStoreId());
        if (StoreType.COMPANY.equals(store.getStoreType())) {
            return;
        }

        Objects.requireNonNull(lastOrderBack);

        if (recordIndex.getBrokerage().compareTo(recordIndex.getDeductBrokerage()) == 0) {
            logger.error("orderRefund orderDetailId={},backBroker equels brokerage", orderDetailId);
            return;
        }

        //退款金额相应佣金
        BigDecimal backBroker = getBroker(lastOrderBack.getBackMoney(), detail.getBrokerageRate()).setScale(0, BigDecimal.ROUND_UP);

        if (backBroker.add(recordIndex.getDeductBrokerage()).compareTo(recordIndex.getBrokerage()) == 1) {

            logger.error("orderRefund orderDetailId={},backBroker={}", orderDetailId, backBroker);

            backBroker = recordIndex.getBrokerage().subtract(recordIndex.getDeductBrokerage());

            logger.error("orderRefund orderDetailId={},backBroker={}", orderDetailId, backBroker);
        }

//        if (backBroker.intValue() < 1) {
//            logger.error("refund orderDetailId={} brokerage is zore ! ", orderDetailId);
//            return;
//        }

        logger.error("orderRefund orderDetailId={},backBroker={}", orderDetailId, backBroker);

        BrokerageSettleStatus prevStatus = recordIndex.getStatus();

        //收货后退款 只累加到售后退款佣金上  只做累加记录
        if (OrderStatus.COMPLETE.equals(mainOrder.getStatus())) {
            logger.error("orderRefund is COMPLETE orderDetailId={},backBroker={}", orderDetailId, backBroker);
            recordIndex.setAfterDeductBrokerage(recordIndex.getAfterDeductBrokerage().add(backBroker));
            brokerageRecordApiService.addAfterDeductBrokerage(recordIndex.getId(), backBroker);

        } else {//收货前退款
            brokerageRecordApiService.refundDeductBrokerage(recordIndex.getId(), mainOrder.getStoreId(), lastOrderBack.getBackOrderNo(), backBroker);
            logger.error("orderRefund  orderDetailId={},backBroker={}", orderDetailId, backBroker);
        }
        AppActionLog.newBuilder(BusinessActionKey.BROKERAGE_RECORD_STATUS_CHANGE).addMap("brokerageRecordId", recordIndex.getId()).addMap("prevStatus", prevStatus).addMap("status", recordIndex.getStatus()).send(actionLogRequest);

    }

    /**
     * 取消订单
     *
     * @param orderDetailIndex the detail
     *
     * @author zhuzz
     * @time 2018 /11/26 10:32:31
     */
    public void brokerage_cancelOrder(OrderDetailIndex orderDetailIndex) {

        logger.error("cancelOrder orderDetailId={}", orderDetailIndex.getId());

        Order mainOrder = orderApiService.findOne(orderDetailIndex.getOrderId());


        PayRecordIndex payRecord = payRecordIndexApiService.findOne(mainOrder.getPayRecordId());

        if (Objects.isNull(payRecord) || !TransactionProgress.PAY_SUCCESS.equals(payRecord.getStatus())) {

            logger.error("cancelOrder orderDetailId={} record illegal pay record not found pay success payRecordId={} ! ", orderDetailIndex.getId(), mainOrder.getPayRecordId());
            return;
        }

        //判断是否是社区半径店铺类型
        Store store = storeApiService.findOne(mainOrder.getStoreId());
        if (StoreType.COMPANY.equals(store.getStoreType())) {
            return;
        }

        BrokerageRecordIndex recordIndex = brokerageRecordIndexApiService.getByOrderId(orderDetailIndex.getId());

        if (Objects.isNull(recordIndex)) {
            logger.error("cancelOrder orderDetailId={} brokerage is null", orderDetailIndex.getId());
            return;
        }

        //当前订单所得佣金
        BigDecimal broker = this.getBroker(orderDetailIndex.getTotalMoney(), orderDetailIndex.getBrokerageRate()).setScale(0, BigDecimal.ROUND_UP);

        if (broker.add(recordIndex.getDeductBrokerage()).compareTo(recordIndex.getBrokerage()) == 1) {

            logger.error("orderRefund orderDetailId={},backBroker={}", recordIndex.getOrderId(), broker);

            broker = recordIndex.getBrokerage().subtract(recordIndex.getDeductBrokerage());

            logger.error("orderRefund orderDetailId={},backBroker={}", recordIndex.getOrderId(), broker);
        }

        if (broker.intValue() < 1) {
            logger.error("cancel order orderDetailId={} brokerage is zore ! ", orderDetailIndex.getId());
            return;
        }




        BrokerageSettleStatus prevStatus = recordIndex.getStatus();
        brokerageRecordApiService.orderCancelBrokerage(recordIndex.getId(), mainOrder.getStoreId(), orderDetailIndex.getOrderNo(), broker);
        AppActionLog.newBuilder(BusinessActionKey.BROKERAGE_RECORD_STATUS_CHANGE).addMap("brokerageRecordId", recordIndex.getId()).addMap("prevStatus", prevStatus).addMap("status", recordIndex.getStatus()).send(actionLogRequest);
        logger.error("cancelOrder orderDetailId={},broker={}", orderDetailIndex.getId(), broker);
    }


    /**
     * 计算佣金
     *
     * @param money the money
     * @param rate  the rate
     *
     * @return the big decimal
     *
     * @author zhuzz
     * @time 2018 /11/26 11:35:11
     */
    private BigDecimal getBroker(BigDecimal money, BigDecimal rate) {
        BigDecimal broker = money.multiply(rate.divide(BigDecimal.valueOf(100)));
        return broker;
    }


    @Override
    public String getConsumerId() {
        return "business_payrecord_status_change";
    }

}

