package cn.lehome.dispatcher.quartz.service.invoke.bpp;

import cn.lehome.base.api.bpp.bean.transaction.BppOrderTransaction;
import cn.lehome.base.api.bpp.bean.transaction.QBppOrderTransaction;
import cn.lehome.base.api.bpp.service.order.BppOrderApiService;
import cn.lehome.base.api.bpp.service.transaction.BppTransactionApiService;
import cn.lehome.base.api.common.pay.bean.CommonResponse;
import cn.lehome.base.api.common.pay.bean.trade.QueryOrderResponse;
import cn.lehome.base.api.common.pay.bean.trade.TradeInfo;
import cn.lehome.base.api.common.pay.service.alipay.isv.ISVAlipayApiService;
import cn.lehome.base.api.common.pay.service.trade.PayTradeApiService;
import cn.lehome.base.api.common.pay.service.wxpay.isp.WXPayISPApiService;
import cn.lehome.bean.bpp.enums.transaction.TransactionStatus;
import cn.lehome.bean.pay.enums.PayStatus;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Service("checkBppOrderService")
public class CheckBppOrderServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private BppTransactionApiService bppTransactionApiService;

    @Autowired
    private PayTradeApiService payTradeApiService;

    @Autowired
    private BppOrderApiService bppOrderApiService;

    @Autowired
    private WXPayISPApiService wxPayISPApiService;

    @Autowired
    private ISVAlipayApiService isvAlipayApiService;


    @Override
    public void doInvoke(Map<String, String> params) {
        Date currentDate = new Date();
        currentDate = DateUtils.addMinutes(currentDate, -30);
        ApiRequest apiRequest = ApiRequest.newInstance().filterEqual(QBppOrderTransaction.status, TransactionStatus.PAYING).filterLessEqual(QBppOrderTransaction.updatedAt, currentDate).filterNotNull(QBppOrderTransaction.outTranNo);
        List<BppOrderTransaction> bppOrderTransactionList = bppTransactionApiService.findAll(apiRequest);
        if (CollectionUtils.isEmpty(bppOrderTransactionList)) {
            logger.error("无需要取消支付流水");
            return;
        }
        for (BppOrderTransaction bppOrderTransaction : bppOrderTransactionList) {
            PayStatus payStatus = queryOrder(bppOrderTransaction.getOutTranNo());
            if (payStatus == null) {
                logger.error("出现错误, outTradeNo={}", bppOrderTransaction.getOutTranNo());
                continue;
            }
            if (payStatus == PayStatus.NOTPAY) {
                boolean result = cancelOrder(bppOrderTransaction.getOutTranNo());
                if (!result) {
                    logger.error("取消收费单失败, outTradeNo={}", bppOrderTransaction.getOutTranNo());
                }
            }
        }

    }

    private PayStatus queryOrder(String outTradeNo) {
        TradeInfo tradeInfo = payTradeApiService.get(outTradeNo);
        if (tradeInfo == null) {
            logger.error("支付记录未找到, outTradeNo = {}", outTradeNo);
            return null;
        }
        if (!tradeInfo.getPayStatus().equals(PayStatus.NOTPAY)) {
            return tradeInfo.getPayStatus();
        }
        QueryOrderResponse response;
        if (tradeInfo.getPayChannel().equals(cn.lehome.bean.pay.enums.PayChannel.WECHAT)) {
            String[] merchantIds = tradeInfo.getMchId().split("-");
            if (merchantIds.length != 2) {
                logger.error("微信支付的商户信息记录错误, outTradeNo = {}, mchId = {}", outTradeNo, tradeInfo.getMchId());
                return null;
            }
            response = wxPayISPApiService.queryOrder(tradeInfo.getOrderId(), tradeInfo.getPaySource(), "", merchantIds[1]);
        } else if (tradeInfo.getPayChannel().equals(cn.lehome.bean.pay.enums.PayChannel.ALIPAY)) {
            response = isvAlipayApiService.queryOrder(tradeInfo.getOrderId(), tradeInfo.getPaySource(), tradeInfo.getMchId());
        } else {
            logger.error("支付类型未知, outTradeNo = {}", outTradeNo);
            return null;
        }
        if (!response.isResStatus()) {
            logger.error("查询订单错误, outTradeNo = {}", outTradeNo);
            return null;
        }
        if (response.getPayStatus().equals(PayStatus.SUCCESS)) {
            payTradeApiService.paySuccess(tradeInfo.getUid());
            bppOrderApiService.paySuccess(tradeInfo.getOrderId());
            return PayStatus.SUCCESS;
        }
        if (response.getPayStatus().equals(PayStatus.PAYERROR)) {
            payTradeApiService.payFailed(tradeInfo.getUid(), "");
            bppOrderApiService.cancelPayOrPayFailed(tradeInfo.getOrderId());
            return PayStatus.PAYERROR;
        }
        if (response.getPayStatus().equals(PayStatus.CLOSED)) {
            payTradeApiService.close(tradeInfo.getUid());
            bppOrderApiService.cancelPayOrPayFailed(tradeInfo.getOrderId());
            return PayStatus.CLOSED;
        }
        return PayStatus.NOTPAY;
    }

    private boolean cancelOrder(String outTradeNo) {
        TradeInfo tradeInfo = payTradeApiService.get(outTradeNo);
        if (tradeInfo == null) {
            logger.error("支付记录未找到, outTradeNo = {}", outTradeNo);
            return false;
        }
        if (!tradeInfo.getPayStatus().equals(PayStatus.NOTPAY)) {
            logger.error("支付记录状态已经改变不能取消, outTradeNo = {}", outTradeNo);
            return false;
        }
        CommonResponse commonResponse = null;
        if (tradeInfo.getPayChannel().equals(cn.lehome.bean.pay.enums.PayChannel.WECHAT)) {
            String[] merchantIds = tradeInfo.getMchId().split("-");
            if (merchantIds.length != 2) {
                logger.error("微信支付的商户信息记录错误, outTradeNo = {}, mchId = {}", outTradeNo, tradeInfo.getMchId());
                return false;
            }
            commonResponse = wxPayISPApiService.closeOrder(tradeInfo.getOrderId(), tradeInfo.getPaySource(), "", merchantIds[1]);
        } else if (tradeInfo.getPayChannel().equals(cn.lehome.bean.pay.enums.PayChannel.ALIPAY)) {
            commonResponse = isvAlipayApiService.closeOrder(tradeInfo.getOrderId(), tradeInfo.getPaySource(), tradeInfo.getMchId());
        } else {
            logger.error("支付类型未知, outTradeNo = {}", outTradeNo);
            return false;
        }
        if (!commonResponse.isResStatus()) {
            logger.error("取消订单失败, outTradeNo = {}", outTradeNo);
            return false;
        }
        bppOrderApiService.cancelPayOrPayFailed(tradeInfo.getOrderId());
        return true;
    }
}
