package cn.lehome.dispatcher.quartz.service.invoke.ecommerce;

import cn.lehome.base.api.business.bean.ecommerce.order.OrderIndex;
import cn.lehome.base.api.business.bean.ecommerce.order.QOrderIndex;
import cn.lehome.base.api.business.bean.ecommerce.pay.PayRecordIndex;
import cn.lehome.base.api.business.bean.ecommerce.store.Store;
import cn.lehome.base.api.business.service.ecommerce.order.OrderApiService;
import cn.lehome.base.api.business.service.ecommerce.order.OrderIndexApiService;
import cn.lehome.base.api.business.service.ecommerce.pay.PayRecordApiService;
import cn.lehome.base.api.business.service.ecommerce.pay.PayRecordIndexApiService;
import cn.lehome.base.api.business.service.ecommerce.store.StoreApiService;
import cn.lehome.base.api.business.service.ecommerce.store.StoreAssetApiService;
import cn.lehome.base.api.tool.compoment.jms.EventBusComponent;
import cn.lehome.bean.business.constants.BusinessActionKey;
import cn.lehome.bean.business.enums.ecommerce.order.OrderStatus;
import cn.lehome.bean.business.enums.ecommerce.order.OrderType;
import cn.lehome.bean.business.enums.ecommerce.pay.TransactionProgress;
import cn.lehome.bean.business.enums.ecommerce.store.StoreStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.actionlog.core.ActionLogRequest;
import cn.lehome.framework.actionlog.core.bean.ActionLog;
import cn.lehome.framework.actionlog.core.bean.AppActionLog;
import cn.lehome.framework.base.api.core.compoment.loader.LoaderServiceComponent;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;

@Service("cancelOrderScheduleJobService")
public class CancelOrderScheduleJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private OrderApiService orderApiService;

    @Autowired
    private OrderIndexApiService orderIndexApiService;

    @Autowired
    private StoreApiService storeApiService;

    @Autowired
    private StoreAssetApiService storeAssetApiService;

    @Autowired
    private PayRecordApiService payRecordApiService;

    @Autowired
    private PayRecordIndexApiService payRecordIndexApiService;

    @Autowired
    private ActionLogRequest actionLogRequest;

    @Autowired
    private LoaderServiceComponent loaderServiceComponent;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Override
    public void doInvoke(Map<String, String> params) {

        logger.error("进入取消订单定时任务");
        boolean flag = true;
        int pageSize = 100;
        Long maxId = 0L;

        while (flag) {
//            try {
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.MINUTE, -20);
            Date time = calendar.getTime();
            ApiResponse<OrderIndex> apiResponse = orderIndexApiService.findAll(ApiRequest.newInstance().filterLike(QOrderIndex.status, OrderStatus.OBLIGATION).filterLessEqual(QOrderIndex.createdTime, time.getTime()).filterGreaterThan(QOrderIndex.id, maxId), ApiRequestPage.newInstance().paging(0, pageSize).addOrder(QOrderIndex.id, PageOrderType.ASC));
            if (!CollectionUtils.isEmpty(apiResponse.getPagedData())) {
                List<OrderIndex> orderIndexList = Lists.newArrayList(apiResponse.getPagedData());
                ActionLog.Builder builder = ActionLog.newBuilder();

                loaderServiceComponent.loadAllBatch(orderIndexList, OrderIndex.class);

                orderIndexList.forEach(p -> {


                    OrderStatus status = OrderStatus.CANCEL;
                    
                    PayRecordIndex payRecordIndex = payRecordIndexApiService.findOne(p.getPayRecordId());
                    if (Objects.nonNull(payRecordIndex)) {
                        if (TransactionProgress.PAY_SUCCESS.equals(payRecordIndex.getStatus())) {
                            status = OrderStatus.WAIT_SHIPMENTS;
                        }
                    }
                    orderApiService.updateStatusForSys(p.getId(), status);

                    if (OrderType.GIFT.equals(p.getOrderType())) {
                        Store store = storeApiService.findByUserAccountId(p.getUserAccountId());
                        if (OrderStatus.CANCEL.equals(status)) {
                            if (Objects.nonNull(store)) {
                                logger.error("删除店铺 store={}", JSON.toJSONString(store));
                                storeApiService.delete(store);
                            }
                        } else if (OrderStatus.WAIT_SHIPMENTS.equals(status)) {
                            storeApiService.updateStatus(store.getId(), StoreStatus.WAIT_CONFIRM);
                        }
                    }


                    builder.addActionLogBean(AppActionLog.newBuilder(BusinessActionKey.ORDER_STATUS_CHANGE).addMap("orderId", p.getId()).addMap("prevOrderStatus", p.getStatus()).addMap("orderStatus", status).build());
                });


                builder.send(actionLogRequest);
                maxId = orderIndexList.get(orderIndexList.size() - 1).getId();
            } else {
                flag = false;
            }
//            } catch (Exception e) {
//                logger.error("订单取消出错 error={}", e.getMessage());
//                flag = false;
//            }
        }


    }

}
