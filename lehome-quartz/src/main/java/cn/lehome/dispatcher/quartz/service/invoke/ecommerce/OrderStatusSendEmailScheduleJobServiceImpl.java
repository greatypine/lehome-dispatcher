package cn.lehome.dispatcher.quartz.service.invoke.ecommerce;

import cn.lehome.base.api.business.bean.ecommerce.order.OrderIndex;
import cn.lehome.base.api.business.bean.ecommerce.order.QOrderIndex;
import cn.lehome.base.api.business.bean.ecommerce.supplier.Supplier;
import cn.lehome.base.api.business.service.ecommerce.order.OrderApiService;
import cn.lehome.base.api.business.service.ecommerce.order.OrderIndexApiService;
import cn.lehome.base.api.business.service.ecommerce.supplier.SupplierApiService;
import cn.lehome.base.api.oauth2.bean.user.UserAccountIndex;
import cn.lehome.base.api.oauth2.service.user.UserAccountIndexApiService;
import cn.lehome.base.api.tool.bean.message.MessageTemplate;
import cn.lehome.base.api.tool.compoment.email.EmailComponent;
import cn.lehome.base.api.tool.constant.MessageKeyConstants;
import cn.lehome.base.api.tool.exception.message.MessageTemplateNotFoundException;
import cn.lehome.base.api.tool.service.message.MessageTemplateApiService;
import cn.lehome.base.api.tool.util.DateUtil;
import cn.lehome.bean.business.enums.ecommerce.order.EmailNoticeStatus;
import cn.lehome.bean.tool.entity.enums.email.BusinessCodeType;
import cn.lehome.bean.tool.entity.enums.email.ServiceCodeType;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.exception.sql.NotFoundRecordException;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.util.StringUtil;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.util.*;

@Service("orderStatusSendEmailScheduleJobService")
public class OrderStatusSendEmailScheduleJobServiceImpl extends AbstractInvokeServiceImpl {


    @Autowired
    private OrderIndexApiService orderIndexApiService;

    @Autowired
    private OrderApiService orderApiService;

    @Autowired
    private EmailComponent emailComponent;

    @Autowired
    private SupplierApiService supplierApiService;

    @Autowired
    private MessageTemplateApiService messageTemplateApiService;

    @Autowired
    private UserAccountIndexApiService userAccountIndexApiService;


    @Override
    public void doInvoke(Map<String, String> params) {


        logger.error("开始进入供应商邮件发送程序！");

        // 订单提醒状态为未发送通知 并且是最近3天的订单信息
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -3);
        Date time = calendar.getTime();

        ApiRequest request = ApiRequest.newInstance();
        request.filterEqual(QOrderIndex.orderNoticeStatus, EmailNoticeStatus.NEED_NOTICE)
                .filterGreaterEqual(QOrderIndex.createdTime, time.getTime());

        List<OrderIndex> orderIndexList = orderIndexApiService.findAll(request);

        logger.error("需要处理的订单数量为{}", orderIndexList.size());

        if (CollectionUtils.isEmpty(orderIndexList)) {
            return;
        }

        ArrayListMultimap<Long, OrderIndex> supplierOrderMap = ArrayListMultimap.create();

        orderIndexList.stream().sorted(Comparator.comparing(OrderIndex::getCreatedTime)).forEach(p -> supplierOrderMap.put(p.getSupplierId(), p));

        MessageTemplate createOrderMsg = getMessageTemplateIdFromKey(MessageKeyConstants.ORDER_CREATE_EMAIL_SEND);
        MessageTemplate cancelOrderMsg = getMessageTemplateIdFromKey(MessageKeyConstants.ORDER_CANCEL_EMAIL_SEND);

        supplierOrderMap.asMap().forEach((k, v) -> {
            Supplier supplier = supplierApiService.get(k);
            if (StringUtil.isBlank(supplier.getEmail())) {
                return;
            }
            int orderNum = v.size();
            HashMap<String, String> subjectMap = Maps.newHashMap();
            subjectMap.put("orderNum", orderNum + "");
            String subject = getContent(createOrderMsg.getName(), subjectMap);

            List<String> contentList = Lists.newArrayList();
            v.forEach(p -> {
                HashMap<String, String> contentMap = Maps.newHashMap();
                contentMap.put("orderTime", DateUtil.getNewFormatDateString(p.getCreatedTime()));
                contentMap.put("orderNo", p.getTotalOrderNo());
                if (Objects.nonNull(p.getOrderMoney())) {
                    contentMap.put("orderMoney", p.getOrderMoney().divide(BigDecimal.valueOf(100)).setScale(BigDecimal.ROUND_HALF_DOWN).toString());
                } else {
                    contentMap.put("orderMoney", "");
                }
                UserAccountIndex userAccount = userAccountIndexApiService.getUserAccount(p.getUserAccountId().toString());
                if (Objects.nonNull(userAccount)) {
                    contentMap.put("userInfo", userAccount.getNickName());
                } else {
                    contentMap.put("userInfo", "");
                }
                String content = "";
                switch (p.getStatus()) {
                    case WAIT_SHIPMENTS:
                        content = createOrderMsg.getContent();
                        break;
                    case CANCEL:
                        content = cancelOrderMsg.getContent();
                        break;
                }
                contentList.add(getContent(content, contentMap));
            });

            String content = String.join("<br/>", contentList);

            //批量更改状态为已发送邮件提醒
            v.forEach(q -> orderApiService.updateOrderNoticeStatus(q.getId(), EmailNoticeStatus.HAVE_NOTICE));
            try {
                logger.error("发送供应商邮件 supplierId={},email={},content={}", supplier.getId(), supplier.getEmail(), content);
                emailComponent.sendEmail(supplier.getEmail(), subject, content, YesNoStatus.YES, ServiceCodeType.BUSINESS, BusinessCodeType.ORDER);
            } catch (Exception ex) {
                //异常状态回滚
                v.forEach(q -> orderApiService.updateOrderNoticeStatus(q.getId(), EmailNoticeStatus.NEED_NOTICE));
            }

        });
        logger.error("供应商邮件发送完成！");
    }


    private MessageTemplate getMessageTemplateIdFromKey(String messageTemplateKey) {
        try {
            MessageTemplate messageTemplate = messageTemplateApiService.findByTemplateKey(messageTemplateKey);
            return messageTemplate;
        } catch (NotFoundRecordException e) {
            throw new MessageTemplateNotFoundException();
        }
    }

    private String getContent(String content, Map<String, String> params) {
        String replaceContent = content;
        if (params != null && !params.isEmpty()) {
            for (String key : params.keySet()) {
                replaceContent = StringUtils.replace(replaceContent, "${" + key + "}", params.get(key));
            }
        }
        return replaceContent;
    }
}
