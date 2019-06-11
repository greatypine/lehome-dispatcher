package cn.lehome.dispatcher.quartz.service.invoke.ecommerce;

import cn.lehome.base.api.business.ec.bean.ecommerce.order.OrderBack;
import cn.lehome.base.api.business.ec.bean.ecommerce.pay.PayRecord;
import cn.lehome.base.api.business.ec.bean.ecommerce.pay.QPayRecordIndex;
import cn.lehome.base.api.business.ec.bean.ecommerce.pay.RefundQueryRequest;
import cn.lehome.base.api.business.ec.bean.ecommerce.pay.RefundQueryResponse;
import cn.lehome.base.api.business.ec.service.ecommerce.order.OrderBackApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.pay.PayApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.pay.PayRecordApiService;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.bean.business.ec.constants.BusinessActionKey;
import cn.lehome.bean.business.ec.enums.ecommerce.order.OrderBackStatus;
import cn.lehome.bean.business.ec.enums.ecommerce.pay.BillType;
import cn.lehome.bean.business.ec.enums.ecommerce.pay.TransactionProgress;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.actionlog.core.ActionLogRequest;
import cn.lehome.framework.actionlog.core.bean.AppActionLog;
import cn.lehome.framework.base.api.core.bean.HttpResponseBean;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import com.alibaba.fastjson.JSON;
import org.apache.commons.httpclient.HttpStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * 支付单中退款订单状态更新调度任务
 * （5分钟调度一次）
 *
 * @author zhuzz
 * @time 2018 /12/03 14:39:53
 */
@Service("orderBackScheduleJobService")
public class OrderBackScheduleJobServiceImpl extends AbstractInvokeServiceImpl {


    @Autowired
    private PayRecordApiService payRecordApiService;

    @Autowired
    private PayApiService payApiService;

    @Autowired
    private OrderBackApiService orderBackApiService;
    @Autowired
    private EventBusComponent eventBusComponent;
    @Autowired
    private ActionLogRequest actionLogRequest;

    /**
     * Do invoke.
     *
     * @param params the params
     *
     * @author zhuzz
     * @time 2018 /12/03 14:39:58
     */
    @Override
    public void doInvoke(Map<String, String> params) {

        logger.error("退款单状态主动更新定时 start！");

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.MINUTE, -3);
        Date time = calendar.getTime();
        ApiRequest apiRequest = ApiRequest.newInstance()
                .filterEqual(QPayRecordIndex.billType, BillType.RETURN)
                .filterEqual(QPayRecordIndex.status, TransactionProgress.PAY_DURING)
                .filterLessThan(QPayRecordIndex.updatedTime, time);

        List<PayRecord> records = payRecordApiService.findAll(apiRequest);

        logger.error("主动查询退款订单集合={}", JSON.toJSONString(records));

        for (PayRecord record : records) {
            RefundQueryRequest refundQueryRequest = new RefundQueryRequest();
            refundQueryRequest.setPayChannel(record.getPayType());
            refundQueryRequest.setRefundId(record.getId());
            HttpResponseBean<RefundQueryResponse> response = payApiService.refundQuery(refundQueryRequest);
            logger.error("退款查询接口定时调用: payRecordId: {} result:{}", record.getId(), JSON.toJSON(response));
            if (HttpStatus.SC_OK == response.getHttpCode()) {
                RefundQueryResponse refundQueryResponse = response.getResponse();
                if (Objects.nonNull(refundQueryResponse)) {
                    if (Boolean.TRUE.equals(refundQueryResponse.getResStatus())) {
                        TransactionProgress transactionProgress = null;
                        OrderBackStatus orderBackStatus = null;
                        switch (refundQueryResponse.getPayStatus()) {
                            case SUCCESS:
                                break;
                            case REFUND:
                                break;
                            case NOTPAY:
                                break;
                            case CLOSED:
                                break;
                            case REVOKED:
                                break;
                            case USERPAYING:
                                break;
                            case PAYERROR:
                                break;
                            case REFUND_SUCCESS:
                                transactionProgress = TransactionProgress.PAY_SUCCESS;
                                orderBackStatus = OrderBackStatus.REIMBURSE_SUCCESS;
                                break;
                            case REFUNDCLOSE:
                                break;
                            case PROCESSING:
                                break;
                            case CHANGE:
                                transactionProgress = TransactionProgress.PAY_FAIL;
                                break;
                        }
                        if (Objects.nonNull(transactionProgress)) {
                            if (!payRecordApiService.updateStatus(record.getId(), transactionProgress)) {
                                logger.error("修改支付单状态错误, payRecordId = " + record.getId());
                                continue;
                            }
                            OrderBack orderBack = orderBackApiService.findByPayRecordId(record.getId());
                            if (orderBack == null) {
                                logger.error("退款单找不到, 不进行处理, payRecordId = " + record.getId());
                                continue;
                            }
                            if (Objects.nonNull(orderBackStatus)) {
                                OrderBackStatus prevStatus = orderBack.getStatus();
                                boolean updateStatusForSys = orderBackApiService.updateStatusForSys(orderBack.getId(), orderBackStatus);
                                if (updateStatusForSys && transactionProgress.equals(TransactionProgress.PAY_SUCCESS)) {
                                    eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.PAYRECORD_STATUS_CHANGE_EVENT, new HashMap.SimpleEntry<>(record.getId(), TransactionProgress.PAY_SUCCESS)));
                                }
                                AppActionLog.newBuilder(BusinessActionKey.ORDER_BACK_STATUS_CHANGE).addMap("orderBackId", orderBack.getOrderDetailId()).addMap("prevOrderBackStatus", prevStatus).addMap("orderBackStatus", orderBackStatus).send(actionLogRequest);
                            }
                        }

                    }
                }
            }


        }
        logger.error("退款单状态主动更新定时 end！");

        logger.error("退款单状态主动退款更新定时 start！");

        apiRequest = ApiRequest.newInstance()
                .filterEqual(QPayRecordIndex.billType, BillType.RETURN)
                .filterEqual(QPayRecordIndex.status, TransactionProgress.CREATE_TRADING_SHEET)
                .filterLessThan(QPayRecordIndex.updatedTime, time);

        records = payRecordApiService.findAll(apiRequest);
        logger.error("主动查询未执行退款订单集合={}", JSON.toJSONString(records));

        for (PayRecord record : records) {
            OrderBack orderBack = orderBackApiService.findByPayRecordId(record.getId());
            if (orderBack == null) {
                logger.error("退款单找不到, 不进行处理, payRecordId = " + record.getId());
                continue;
            }
            if (!orderBack.getStatus().equals(OrderBackStatus.AFFIRM_REIMBURSE)) {
                logger.error("退款单状态不是确认退款, 不进行处理, orderBackId = " + orderBack.getId());
                continue;
            }
            if (!payRecordApiService.updateStatus(record.getId(), TransactionProgress.PAY_DURING)) {
                logger.error("修改支付单状态错误, payRecordId = " + record.getId());
                continue;
            }
            payRecordApiService.forcedRefund(record.getId());
        }
        logger.error("退款单状态主动退款更新定时 end！");


    }
}
