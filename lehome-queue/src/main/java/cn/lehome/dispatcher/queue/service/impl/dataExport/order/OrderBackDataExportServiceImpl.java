package cn.lehome.dispatcher.queue.service.impl.dataExport.order;

import cn.lehome.base.api.business.ec.bean.ecommerce.order.*;
import cn.lehome.base.api.business.ec.service.ecommerce.order.OrderBackIndexApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.order.OrderDetailIndexApiService;
import cn.lehome.base.api.business.ec.service.ecommerce.order.OrderIndexApiService;
import cn.lehome.base.api.common.bean.dataexport.DataExportRecord;
import cn.lehome.base.api.common.constant.ExportDataBusinessConstants;
import cn.lehome.bean.business.ec.enums.ecommerce.order.BackReason;
import cn.lehome.dispatcher.queue.service.impl.dataExport.AbstractDataExportServiceImpl;
import cn.lehome.framework.base.api.core.bean.BaseApiBean;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zuoguodong on 2019/9/26
 */
@Service("orderBackDataExportService")
public class OrderBackDataExportServiceImpl extends AbstractDataExportServiceImpl{

    @Autowired
    private OrderBackIndexApiService orderBackIndexApiService;

    @Autowired
    private OrderDetailIndexApiService orderDetailIndexApiService;

    @Autowired
    private OrderIndexApiService orderIndexApiService;

    @Override
    public DataExportRecord exportData(DataExportRecord dataExportRecord, Long pageIndex) {
        OrderBackExportRequest orderBackExportRequest = JSON.parseObject(dataExportRecord.getQueryStr(),OrderBackExportRequest.class);
        ApiRequest apiRequest = ApiRequest.newInstance();
        if(orderBackExportRequest != null) {
            if (StringUtils.isNotEmpty(orderBackExportRequest.getOrderDetailNo())) {
                apiRequest.filterEqual(QOrderBackIndex.orderDetailNo, orderBackExportRequest.getOrderDetailNo());
            }
            if (orderBackExportRequest.getStartTime() != null) {
                apiRequest.filterGreaterThan(QOrderBackIndex.createdTime, orderBackExportRequest.getStartTime());
            }
            if (orderBackExportRequest.getEndTime() != null) {
                apiRequest.filterLessThan(QOrderBackIndex.createdTime, orderBackExportRequest.getEndTime());
            }
            if (StringUtils.isNotEmpty(orderBackExportRequest.getBackOrderNo())) {
                apiRequest.filterEqual(QOrderBackIndex.backOrderNo, orderBackExportRequest.getBackOrderNo());
            }
            if (orderBackExportRequest.getStatus() != null) {
                apiRequest.filterLike(QOrderBackIndex.status, orderBackExportRequest.getStatus());
            }
            if (orderBackExportRequest.getRefundType() != null) {
                apiRequest.filterLike(QOrderBackIndex.refundType, orderBackExportRequest.getRefundType());
            }
        }
        ApiResponse<OrderBackIndex> orderBackIndexApiResponse =  orderBackIndexApiService.findAll(
                apiRequest, ApiRequestPage.newInstance().paging(pageIndex.intValue(), 10));
        List<OrderBackCMSResponse> orderBackCMSResponseList = Lists.newArrayList();
        List<OrderBackIndex> orderBackIndexList = Lists.newArrayList(orderBackIndexApiResponse.getPagedData());
        if(!CollectionUtils.isEmpty(orderBackIndexList)) {
            for(OrderBackIndex orderBackIndex : orderBackIndexList) {
                OrderIndex orderIndex = orderIndexApiService.findOne(orderBackIndex.getOrderId());
                OrderBackCMSResponse orderBackCMSResponse = BeanMapping.map(orderBackIndex, OrderBackCMSResponse.class);
                if (orderIndex != null) {
                    orderBackCMSResponse.setOrderDetailMoney(orderIndex.getOrderMoney().toString());
                    orderBackCMSResponse.setTotalOrderNo(orderIndex.getTotalOrderNo());
                }
                if (orderBackIndex.getOrderDetailId() != 0L) {
                    if (!orderBackIndex.getBackReason().equals(BackReason.ORDER_CANCEL)) {
                        OrderDetailIndex orderDetailIndex = orderDetailIndexApiService.findByOrderNo(orderBackIndex.getOrderDetailNo());
                        if (orderDetailIndex != null) {
                            orderBackCMSResponse.setGoodsSpuName(orderBackIndex.getGoodsSpuName());
                            orderBackCMSResponse.setOrderDetailMoney(orderDetailIndex.getTotalMoney().toString());
                            orderBackCMSResponse.setRefundMoney(orderDetailIndex.getRefundMoney() == null ? "0" : orderDetailIndex.getRefundMoney().toString());
                        }
                    }
                }
                orderBackCMSResponseList.add(orderBackCMSResponse);
            }
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<List<String>> rowList = new ArrayList<>();
        orderBackCMSResponseList.forEach(orderBack ->{
            List<String> row = new ArrayList<>();
            row.add(orderBack.getBackOrderNo());
            row.add(orderBack.getTotalOrderNo());
            row.add(orderBack.getOrderDetailNo());
            row.add(orderBack.getGoodsSpuName()+"(" + orderBack.getGoodsSkuName() + ")");
            row.add(orderBack.getRefundType().getName());
            row.add("￥" + (StringUtils.isEmpty(orderBack.getOrderDetailMoney())?"0":new BigDecimal(orderBack.getOrderDetailMoney()).divide(new BigDecimal("100"),2)));
            row.add("￥" + (StringUtils.isEmpty(orderBack.getBackMoney())?"0":new BigDecimal(orderBack.getBackMoney()).divide(new BigDecimal("100"),2)));
            row.add(simpleDateFormat.format(orderBack.getCreatedTime()));
            row.add(orderBack.getStatus().getName());
            rowList.add(row);
        });
        this.appendExcelData(dataExportRecord,rowList);
        return dataExportRecord;
    }

    @Override
    public String[] getTitle() {
        return new String[]{
                "退款编号",
                "订单号",
                "子订单号",
                "商品名称",
                "退款方式",
                "订单金额",
                "退款金额",
                "申请时间",
                "退款状态"
        };
    }

    @Override
    public String getFileName() {
        return "退货单列表导出";
    }
}
