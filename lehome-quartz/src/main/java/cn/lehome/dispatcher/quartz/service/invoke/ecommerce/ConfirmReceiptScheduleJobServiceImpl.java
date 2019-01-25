package cn.lehome.dispatcher.quartz.service.invoke.ecommerce;

import cn.lehome.base.api.business.bean.ecommerce.order.Order;
import cn.lehome.base.api.business.bean.ecommerce.order.OrderBack;
import cn.lehome.base.api.business.bean.ecommerce.order.OrderDetailIndex;
import cn.lehome.base.api.business.bean.ecommerce.order.QOrderIndex;
import cn.lehome.base.api.business.bean.ecommerce.store.BrokerageRecordIndex;
import cn.lehome.base.api.business.service.ecommerce.order.OrderApiService;
import cn.lehome.base.api.business.service.ecommerce.order.OrderBackApiService;
import cn.lehome.base.api.business.service.ecommerce.order.OrderDetailIndexApiService;
import cn.lehome.base.api.business.service.ecommerce.store.BrokerageRecordApiService;
import cn.lehome.bean.business.constants.BusinessActionKey;
import cn.lehome.bean.business.enums.ecommerce.order.OrderBackStatus;
import cn.lehome.bean.business.enums.ecommerce.order.OrderStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.actionlog.core.ActionLogRequest;
import cn.lehome.framework.actionlog.core.bean.AppActionLog;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service("confirmReceiptScheduleJobService")
public class ConfirmReceiptScheduleJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private OrderApiService orderApiService;

    @Autowired
    private ActionLogRequest actionLogRequest;

    @Autowired
    private BrokerageRecordApiService brokerageRecordApiService;

    @Autowired
    private OrderDetailIndexApiService orderDetailIndexApiService;

    @Autowired
    private OrderBackApiService orderBackApiService;

    @Override
    public void doInvoke(Map<String, String> params) {

        logger.error("进入确认收货定时任务");
        int pageSize = 100;
        Long maxId = 0L;

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -7);
        Date time = calendar.getTime();

        ApiRequestPage page = ApiRequestPage.newInstance().paging(0, pageSize).addOrder(QOrderIndex.id, PageOrderType.ASC);

        while (true) {
            ApiResponse<Order> apiResponse = orderApiService.findAll(ApiRequest.newInstance().filterEqual(QOrderIndex.status, OrderStatus.WAIT_RECEIVE).filterLessEqual(QOrderIndex.updatedTime, time), page);

            if (apiResponse == null || apiResponse.getCount() == 0) {
                break;
            }

            List<Order> orderList = Lists.newArrayList(apiResponse.getPagedData());

            for (Order order : orderList) {
                try {
                    logger.error("需要自动确认订单, id = " + order.getId());
                    if (order.getPostponeDays() != 0) {
                        Date tempTime = DateUtils.addDays(time, -order.getPostponeDays());
                        if (tempTime.getTime() < order.getUpdatedTime().getTime()) {
                            logger.error("未到确认订单时间, orderId = " + order.getId());
                            continue;
                        }
                    }
                    confirmOrder(order);
                } catch (Exception e) {
                    logger.error("自动确认订单失败, orderId = " + order.getId(), e);
                }
            }

            if (apiResponse.getCount() < page.getPageSize()) {
                break;
            }

            page.pagingNext();
        }


    }

    private void confirmOrder(Order order) {
        boolean isHasUnfinishOrderBack = false;
        List<OrderBack> orderBackList = orderBackApiService.findByOrderId(order.getId());
        if (!CollectionUtils.isEmpty(orderBackList)) {
            for (OrderBack orderBack : orderBackList) {
                if (OrderBackStatus.hasProblemStatus(orderBack.getStatus())) {
                    isHasUnfinishOrderBack = true;
                }
            }
        }

        if (isHasUnfinishOrderBack) {
            logger.error("订单含有未完成退款单不能确认订单, orderId = " + order);
            return;
        }

        boolean updateStatusForSys = orderApiService.updateStatusForSys(order.getId(), OrderStatus.COMPLETE);
        if (updateStatusForSys) {
            List<OrderDetailIndex> orderDetails = orderDetailIndexApiService.findByOrderId(order.getId());
            for (OrderDetailIndex orderDetailIndex : orderDetails) {
                BrokerageRecordIndex brokerageRecordIndex = brokerageRecordApiService.orderCompleteBrokerage(orderDetailIndex);
                AppActionLog.newBuilder(BusinessActionKey.BROKERAGE_RECORD_STATUS_CHANGE).addMap("brokerageRecordId", brokerageRecordIndex.getId()).send(actionLogRequest);
            }
            AppActionLog.newBuilder(BusinessActionKey.ORDER_STATUS_CHANGE, order.getUserOpenId(), order.getClientId()).addMap("orderId", order.getId()).addMap("orderStatus",OrderStatus.COMPLETE).send(actionLogRequest);
        }
    }

    public static void main(String[] args) {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -7);
        Date time = calendar.getTime();
        System.out.println(time.getTime());
    }
}
