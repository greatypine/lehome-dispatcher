package cn.lehome.dispatcher.utils.order;

import cn.lehome.base.api.business.bean.ecommerce.order.OrderIndex;
import cn.lehome.base.api.business.bean.ecommerce.order.QOrderIndex;
import cn.lehome.base.api.business.service.ecommerce.order.OrderIndexApiService;
import cn.lehome.bean.business.enums.ecommerce.order.OrderStatus;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by zuoguodong on 2019/1/11
 */
@Service("orderService")
public class OrderServiceImpl implements OrderService{

    private static final String ORDER_CHANGE_KEY = "00000024";

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Autowired
    private OrderIndexApiService orderIndexApiService;

    @Override
    public void refreshOrderDate(String[] input) {
        if(input.length < 1){
            System.out.println("参数错误");
            return;
        }
        if(input.length > 2){
            for(int i = 1;i<input.length;i++){
                Long orderId = Long.valueOf(input[i]);
                OrderIndex orderIndex = orderIndexApiService.findOne(orderId);
                Long payTime = getTime(orderId,OrderStatus.WAIT_SHIPMENTS.getName());
                boolean isupdate = false;
                if(payTime != null) {
                    orderIndex.setPayTime(new Date(payTime));
                    isupdate = true;
                }
                Long deliverTime = getTime(orderId,OrderStatus.WAIT_RECEIVE.getName());
                if(deliverTime != null) {
                    orderIndex.setDeliveryTime(new Date(deliverTime));
                    isupdate = true;
                }
                if(isupdate) {
                    orderIndexApiService.update(orderIndex);
                }
            }
        }else{
            int start = 0;
            int pageSize = 100;
            List<OrderStatus> status = Lists.newArrayList(OrderStatus.OBLIGATION);
            ApiResponse<OrderIndex> response = orderIndexApiService.findAll(ApiRequest.newInstance().filterNotIn(QOrderIndex.status,status), ApiRequestPage.newInstance().paging(start,pageSize));
            List<OrderIndex> list = new ArrayList<>(response.getPagedData());
            while(!list.isEmpty()){
                list.forEach(orderIndex -> {
                    boolean isupdate = false;
                    Long payTime = getTime(orderIndex.getId(),OrderStatus.WAIT_SHIPMENTS.getName());
                    if(payTime != null) {
                        orderIndex.setPayTime(new Date(payTime));
                        isupdate = true;
                    }
                    Long deliverTime = getTime(orderIndex.getId(),OrderStatus.WAIT_RECEIVE.getName());
                    if(deliverTime != null) {
                        orderIndex.setDeliveryTime(new Date(deliverTime));
                        isupdate = true;
                    }
                    if(isupdate) {
                        orderIndexApiService.update(orderIndex);
                    }
                });
                start ++;
                response = orderIndexApiService.findAll(ApiRequest.newInstance().filterNotIn(QOrderIndex.status,status), ApiRequestPage.newInstance().paging(start,pageSize));
                list = new ArrayList<>(response.getPagedData());
            }
        }
    }

    private Long getTime(Long orderId,String orderStatus){
        return hbaseTemplate.execute("action_log", hTable -> {
            Scan scan = new Scan();
            Filter keyFilter = new PrefixFilter(Bytes.toBytes(ORDER_CHANGE_KEY));
            Filter idFilter = new SingleColumnValueFilter(Bytes.toBytes("properties"), Bytes.toBytes("orderId"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(orderId.toString()));
            Filter stautsFilter = new SingleColumnValueFilter(Bytes.toBytes("properties"),Bytes.toBytes("orderStatus"),CompareFilter.CompareOp.EQUAL, Bytes.toBytes(orderStatus));
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            filterList.addFilter(keyFilter);
            filterList.addFilter(idFilter);
            filterList.addFilter(stautsFilter);
            scan.setFilter(filterList);
            ResultScanner results = hTable.getScanner(scan);
            Result result = results.next();
            if(!result.isEmpty()){
                return result.getColumnCells(Bytes.toBytes("base"),Bytes.toBytes("action_key")).get(0).getTimestamp();
            }
            return null;
        });
    }

}
