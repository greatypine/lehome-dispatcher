package cn.lehome.dispatcher.queue.listener.order;

import cn.lehome.base.api.business.service.ecommerce.order.OrderBackIndexApiService;
import cn.lehome.base.api.business.service.ecommerce.order.OrderDetailApiService;
import cn.lehome.base.api.business.service.ecommerce.order.OrderIndexApiService;
import cn.lehome.base.api.business.service.ecommerce.pay.PayRecordIndexApiService;
import cn.lehome.base.api.business.service.ecommerce.store.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:base-api.xml")
public class OrderStatusChangeListenerTest {

    @Autowired
    private StoreAssetApiService storeAssetApiService;
    @Autowired
    private StoreApiService storeApiService;
    @Autowired
    private OrderIndexApiService orderIndexApiService;
    //    @Autowired
//    private OrderDetailIndexApiService orderDetailIndexApiService;
    @Autowired
    private OrderDetailApiService orderDetailApiService;
    @Autowired
    private BrokerageRecordIndexApiService brokerageRecordIndexApiService;
    @Autowired
    private BrokerageRecordApiService brokerageRecordApiService;
    @Autowired
    private OrderBackIndexApiService orderBackIndexApiService;
    @Autowired
    private StoreAssetFlowApiService storeAssetFlowApiService;
    @Autowired
    private PayRecordIndexApiService payRecordIndexApiService;

    Long orderDetailId = 1L;

    @Test
    public void payComplete() {
//        orderStatusChangeListener.payComplete(orderDetailId);
    }

    @Test
    public void cancelOrder() {
//        orderStatusChangeListener.cancelOrder(orderDetailId);
    }

    @Test
    public void orderComplete() {
//        orderStatusChangeListener.orderComplete(orderDetailId);
    }

    @Test
    public void refund() {
//        orderStatusChangeListener.refund(orderDetailId);
    }
}