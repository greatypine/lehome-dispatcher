package cn.lehome.dispatcher.quartz.service.invoke.bpp;

import cn.lehome.base.api.bpp.bean.bill.*;
import cn.lehome.base.api.bpp.bean.fee.BppFee;
import cn.lehome.base.api.bpp.bean.fee.BppFeeScale;
import cn.lehome.base.api.bpp.bean.order.*;
import cn.lehome.base.api.bpp.bean.setting.BppSetting;
import cn.lehome.base.api.bpp.bean.setting.QBppSetting;
import cn.lehome.base.api.bpp.bean.transaction.BppOrderTransaction;
import cn.lehome.base.api.bpp.bean.transaction.BppOrderTransactionIndex;
import cn.lehome.base.api.bpp.bean.transaction.QBppOrderTransaction;
import cn.lehome.base.api.bpp.service.bill.BppBillApiService;
import cn.lehome.base.api.bpp.service.bill.BppBillIndexApiService;
import cn.lehome.base.api.bpp.service.fee.BppFeeApiService;
import cn.lehome.base.api.bpp.service.order.BppOrderApiService;
import cn.lehome.base.api.bpp.service.order.BppOrderIndexApiService;
import cn.lehome.base.api.bpp.service.setting.BppSettingApiService;
import cn.lehome.base.api.bpp.service.transaction.BppTransactionApiService;
import cn.lehome.base.api.common.business.oauth2.bean.user.*;
import cn.lehome.base.api.common.business.oauth2.service.user.UserAccountIndexApiService;
import cn.lehome.base.api.common.operation.bean.application.Applications;
import cn.lehome.base.api.common.operation.bean.application.ApplicationsTenant;
import cn.lehome.base.api.common.operation.bean.application.QApplicationsTenant;
import cn.lehome.base.api.common.operation.service.application.ApplicationApiService;
import cn.lehome.base.api.common.operation.service.application.ApplicationsTenantApiService;
import cn.lehome.base.pro.api.bean.area.AreaInfo;
import cn.lehome.base.pro.api.bean.house.HouseInfoIndex;
import cn.lehome.base.pro.api.bean.house.QHouseInfoIndex;
import cn.lehome.base.pro.api.bean.households.HouseholdIndex;
import cn.lehome.base.pro.api.bean.households.QHouseholdIndex;
import cn.lehome.base.pro.api.service.area.AreaInfoApiService;
import cn.lehome.base.pro.api.service.house.HouseInfoIndexApiService;
import cn.lehome.base.pro.api.service.households.HouseholdIndexApiService;
import cn.lehome.bean.bpp.enums.bill.BillPaidType;
import cn.lehome.bean.bpp.enums.bill.BillStatus;
import cn.lehome.bean.bpp.enums.order.OrderStatus;
import cn.lehome.bean.bpp.enums.setting.BppSettingType;
import cn.lehome.bean.bpp.enums.transaction.PayType;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2019/9/16.
 */
@Service("bppFileService")
public class BppFileServiceImpl extends AbstractInvokeServiceImpl {

    private static final String BPP_KEY = "pbpp";

    @Autowired
    private ApplicationApiService applicationApiService;

    @Autowired
    private ApplicationsTenantApiService applicationsTenantApiService;

    @Autowired
    private BppSettingApiService bppSettingApiService;

    @Value("${bpp.file.days}")
    private Integer defaultFileDays;

    @Autowired
    private BppBillApiService bppBillApiService;

    @Autowired
    private BppFeeApiService bppFeeApiService;

    @Autowired
    private HouseInfoIndexApiService proHouseInfoIndexApiService;

    @Autowired
    private BppOrderApiService bppOrderApiService;

    @Autowired
    private UserAccountIndexApiService businessUserAccountIndexApiService;

    @Autowired
    private BppBillIndexApiService bppBillIndexApiService;

    @Autowired
    private AreaInfoApiService proAreaInfoApiService;

    @Autowired
    private HouseholdIndexApiService proHouseholdIndexApiService;

    @Autowired
    private BppTransactionApiService bppTransactionApiService;

    @Autowired
    private BppOrderIndexApiService bppOrderIndexApiService;

    @Override
    public void doInvoke(Map<String, String> params) {
        List<Applications> applicationsList = applicationApiService.findByKey(Sets.newHashSet(BPP_KEY));
        if (CollectionUtils.isEmpty(applicationsList)) {
            logger.error("应用信息未找到");
            return;
        }
        List<ApplicationsTenant> applicationsTenants = applicationsTenantApiService.findAll(ApiRequest.newInstance().filterEqual(QApplicationsTenant.applicationId, applicationsList.get(0).getId()));
        if (!CollectionUtils.isEmpty(applicationsTenants)) {
            for (ApplicationsTenant applicationsTenant : applicationsTenants) {
                logger.error("开始财务归档任务, areaId = {}", applicationsTenant.getObjectId());
                Integer areaId = Integer.valueOf(applicationsTenant.getObjectId());
                List<BppSetting> bppSettings = bppSettingApiService.findAll(ApiRequest.newInstance().filterEqual(QBppSetting.type, BppSettingType.CLOSE_BILL).filterEqual(QBppSetting.areaId, areaId));
                Integer days = defaultFileDays;
                if (!CollectionUtils.isEmpty(bppSettings)) {
                    days = Integer.valueOf(bppSettings.get(0).getConfig());
                }
                this.fileBill(days, areaId);
                this.fileOrder(days, areaId);
                this.filePayOrder(days, areaId);
                logger.error("结束财务归档任务, areaId = {}", applicationsTenant.getObjectId());

            }
        }
    }

    private void filePayOrder(Integer days, Integer areaId) {
        Date date = new Date();
        date = DateUtils.addDays(date, -days);
        date = DateUtils.setDays(date, 1);
        ApiRequest apiRequest = ApiRequest.newInstance().filterLessThan(QBppPayOrder.createdAt, date).filterEqual(QBppPayOrder.areaId, areaId);
        ApiRequestPage requestPage = ApiRequestPage.newInstance().paging(0, 100);
        Integer errorNum = 0;
        Integer fileNum = 0;
        Integer notDeleteNum = 0;
        while (true) {
            ApiResponse<BppPayOrder> response = bppOrderApiService.findPayAll(apiRequest, requestPage);

            if (response == null || response.getCount() == 0) {
                break;
            }
            for (BppPayOrder bppPayOrder : response.getPagedData()) {
                try {
                    BppPayOrderIndex bppPayOrderIndex = BeanMapping.map(bppPayOrder, BppPayOrderIndex.class);
                    BppFee bppFee = bppFeeApiService.getFee(bppPayOrderIndex.getFeeId());
                    AreaInfo areaInfo = proAreaInfoApiService.findOne(bppPayOrderIndex.getAreaId());
                    if (bppFee != null) {
                        bppPayOrderIndex.setFeeName(bppFee.getName());
                    }
                    if (areaInfo != null) {
                        bppPayOrderIndex.setAreaName(areaInfo.getAreaName());
                    }
                    BppOrderOtherBusinessRelation bppOrderOtherBusinessRelation = bppOrderApiService.findByOrderId(YesNoStatus.YES, bppPayOrder.getId());
                    if (bppOrderOtherBusinessRelation != null) {
                        bppPayOrderIndex.setBusinessType(bppOrderOtherBusinessRelation.getBusinessType());
                        bppPayOrderIndex.setBusinessId(bppOrderOtherBusinessRelation.getBusinessId());
                        bppPayOrderIndex.setRelationOrderNumber(bppOrderOtherBusinessRelation.getRelationOrderNumber());
                    }
                    if (bppPayOrderIndex.getOrderStatus().equals(OrderStatus.CREATE)) {
                        bppOrderIndexApiService.createPayOrder(bppPayOrderIndex, false);
                        notDeleteNum++;
                    } else {
                        bppOrderIndexApiService.createPayOrder(bppPayOrderIndex, true);
                        fileNum++;
                    }

                } catch (Exception e) {
                    logger.error("归档支出单信息错误, orderId = {} :", bppPayOrder.getId(), e);
                    errorNum++;
                }
            }
            logger.error("正常导入支出单{}条, 导入未删除支出单{}条, 导入支出单错误{}条", fileNum, notDeleteNum, errorNum);

            if (response.getCount() < response.getPageSize()) {
                break;
            }
            requestPage.pagingNext();
        }
    }

    private void fileOrder(Integer days, Integer areaId) {
        Date date = new Date();
        date = DateUtils.addDays(date, -days);
        date = DateUtils.setDays(date, 1);
        ApiRequest apiRequest = ApiRequest.newInstance().filterLessThan(QBppOrder.createdAt, date).filterEqual(QBppOrder.areaId, areaId);
        ApiRequestPage requestPage = ApiRequestPage.newInstance().paging(0, 100);
        Integer errorNum = 0;
        Integer fileNum = 0;
        Integer notDeleteNum = 0;
        while (true) {
            ApiResponse<BppOrder> response = bppOrderApiService.findAll(apiRequest, requestPage);

            if (response == null || response.getCount() == 0) {
                break;
            }
            for (BppOrder bppOrder : response.getPagedData()) {
                try {
                    BppOrderIndex bppOrderIndex = BeanMapping.map(bppOrder, BppOrderIndex.class);
                    BppFee bppFee = bppFeeApiService.getFee(bppOrder.getFeeId());
                    List<HouseInfoIndex> houseInfoIndices = proHouseInfoIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseInfoIndex.addressId, bppOrder.getAddressId()));
                    AreaInfo areaInfo = proAreaInfoApiService.findOne(bppOrder.getAreaId());
                    if (bppFee != null) {
                        bppOrderIndex.setFeeName(bppFee.getName());
                    }
                    if (!CollectionUtils.isEmpty(houseInfoIndices)) {
                        bppOrderIndex.setAddress(houseInfoIndices.get(0).getRoomAddress());
                    }
                    if (areaInfo != null) {
                        bppOrderIndex.setAreaName(areaInfo.getAreaName());
                    }
                    BppOrderOtherBusinessRelation bppOrderOtherBusinessRelation = bppOrderApiService.findByOrderId(YesNoStatus.NO, bppOrder.getId());
                    if (bppOrderOtherBusinessRelation != null) {
                        bppOrderIndex.setBusinessType(bppOrderOtherBusinessRelation.getBusinessType());
                        bppOrderIndex.setBusinessId(bppOrderOtherBusinessRelation.getBusinessId());
                        bppOrderIndex.setRelationOrderNumber(bppOrderOtherBusinessRelation.getRelationOrderNumber());
                    }
                    if (bppOrder.getOrderStatus().equals(OrderStatus.PAID)) {
                        if (bppOrder.getIsCustom() != null && bppOrder.getIsCustom().equals(YesNoStatus.YES)) {
                            List<HouseholdIndex> householdIndices = proHouseholdIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseholdIndex.userId, bppOrder.getChargeUserOpenId()));
                            if (!CollectionUtils.isEmpty(householdIndices)) {
                                bppOrderIndex.setChargeUserName(householdIndices.get(0).getName());
                            }
                        } else {
                            if (StringUtils.isNotEmpty(bppOrder.getChargeUserOpenId())) {
                                List<Oauth2AccountIndex> oauth2AccountIndexList = businessUserAccountIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QOauth2Account.clientId, "sqbj-smart").filterEqual(QOauth2Account.userOpenId, bppOrder.getChargeUserOpenId()));
                                if (!CollectionUtils.isEmpty(oauth2AccountIndexList)) {
                                    UserAccountIndex userAccountIndex = businessUserAccountIndexApiService.getUserAccount(Long.valueOf(oauth2AccountIndexList.get(0).getAccountId()));
                                    if (userAccountIndex != null) {
                                        bppOrderIndex.setChargeUserName(userAccountIndex.getRealName());
                                    }
                                }
                            }
                        }
                    }

                    List<BppOrderTransaction> bppOrderTransactions = bppTransactionApiService.findAll(ApiRequest.newInstance().filterEqual(QBppOrderTransaction.orderId, bppOrder.getId()));
                    List<BppOrderTransactionIndex> orderTransactions = Lists.newArrayList();
                    if (!CollectionUtils.isEmpty(bppOrderTransactions)) {
                        List<String> accountOpenIds = bppOrderTransactions.stream().filter(orderTransaction -> !orderTransaction.getPayType().equals(PayType.ALIPAY) && !orderTransaction.getPayType().equals(PayType.WXPAY)).filter(bppOrderTransaction -> StringUtils.isNotEmpty(bppOrderTransaction.getOpenId())).map(BppOrderTransaction::getOpenId).collect(Collectors.toList());
                        List<String> householdUserIds = bppOrderTransactions.stream().filter(orderTransaction -> orderTransaction.getPayType().equals(PayType.ALIPAY) || orderTransaction.getPayType().equals(PayType.WXPAY)).filter(bppOrderTransaction -> StringUtils.isNotEmpty(bppOrderTransaction.getOpenId())).map(BppOrderTransaction::getOpenId).collect(Collectors.toList());
                        Map<String, UserAccountIndex> userAccountIndexMap = Maps.newHashMap();
                        Map<String, HouseholdIndex> householdIndexMap = Maps.newHashMap();
                        if (!CollectionUtils.isEmpty(accountOpenIds)) {
                            List<Oauth2AccountIndex> oauth2AccountIndexList = businessUserAccountIndexApiService.findAll(ApiRequest.newInstance().filterIn(QOauth2AccountIndex.userOpenId, accountOpenIds).filterEqual(QOauth2AccountIndex.clientId, "sqbj-smart"));
                            Map<String, UserAccountIndex> accountIndexMap = Maps.newConcurrentMap();
                            if (!CollectionUtils.isEmpty(oauth2AccountIndexList)) {
                                List<UserAccountIndex> userAccountIndices = businessUserAccountIndexApiService.findAccountAll(ApiRequest.newInstance().filterIn(QOauth2AccountIndex.id, oauth2AccountIndexList.stream().map(Oauth2AccountIndex::getAccountId).collect(Collectors.toList())));
                                if (!CollectionUtils.isEmpty(userAccountIndices)) {
                                    userAccountIndices.forEach(userAccountIndex -> accountIndexMap.put(userAccountIndex.getId(), userAccountIndex));
                                }
                            }
                            for (Oauth2AccountIndex oauth2AccountIndex : oauth2AccountIndexList) {
                                UserAccountIndex userAccountIndex = accountIndexMap.get(oauth2AccountIndex.getAccountId());
                                if (userAccountIndex != null) {
                                    userAccountIndexMap.put(oauth2AccountIndex.getUserOpenId(), userAccountIndex);
                                }
                            }
                        }
                        if (!CollectionUtils.isEmpty(householdUserIds)) {
                            List<HouseholdIndex> householdIndices = proHouseholdIndexApiService.findAll(ApiRequest.newInstance().filterIn(QHouseholdIndex.userId, householdUserIds).filterEqual(QHouseholdIndex.areaId, bppOrder.getAreaId()));
                            if (!CollectionUtils.isEmpty(householdIndices)) {
                                householdIndices.forEach(householdIndex -> householdIndexMap.put(householdIndex.getUserId().toString(), householdIndex));
                            }
                        }
                        for (BppOrderTransaction bppOrderTransaction : bppOrderTransactions) {
                            BppOrderTransactionIndex orderTransaction = BeanMapping.map(bppOrderTransaction, BppOrderTransactionIndex.class);
                            if (!orderTransaction.getPayType().equals(PayType.ALIPAY) && !orderTransaction.getPayType().equals(PayType.WXPAY)) {
                                UserAccountIndex userAccountIndex = userAccountIndexMap.get(bppOrderTransaction.getOpenId());
                                if (userAccountIndex != null) {
                                    orderTransaction.setOperationUserName(userAccountIndex.getRealName());
                                }
                            } else {
                                HouseholdIndex householdIndex = householdIndexMap.get(bppOrderTransaction.getOpenId());
                                if (householdIndex != null) {
                                    orderTransaction.setOperationUserName(householdIndex.getName());
                                }
                            }
                            orderTransactions.add(orderTransaction);
                        }
                    }
                    List<BppOrderDetail> bppOrderDetails = bppOrderApiService.findByOrderId(bppOrder.getId());
                    List<BppOrderDetailIndex> list = Lists.newArrayList();
                    if (!CollectionUtils.isEmpty(bppOrderDetails)) {
                        BppFeeScale bppFeeScale = bppFeeApiService.getFeeScale(bppOrderDetails.get(0).getScaleId());
                        for (BppOrderDetail bppOrderDetail : bppOrderDetails) {
                            BppOrderDetailIndex bppOrderDetailIndex = BeanMapping.map(bppOrderDetail, BppOrderDetailIndex.class);
                            bppOrderDetailIndex.setFeeName(bppFee.getName());
                            bppOrderDetailIndex.setScaleName(bppFeeScale.getName());
                            list.add(bppOrderDetailIndex);
                        }
                    }
                    if (bppOrder.getOrderStatus().equals(OrderStatus.CREATE) || bppOrder.getOrderStatus().equals(OrderStatus.PAYING) || bppOrder.getOrderStatus().equals(OrderStatus.REFUNDING)) {
                        bppOrderIndexApiService.crateOrder(bppOrderIndex, list, orderTransactions, false);
                        fileNum++;
                    } else {
                        bppOrderIndexApiService.crateOrder(bppOrderIndex, list, orderTransactions, true);
                        notDeleteNum++;
                    }

                } catch (Exception e) {
                    logger.error("归档收费单信息错误, orderId = {} :", bppOrder.getId(), e);
                    errorNum++;
                }
            }
            logger.error("正常导入支付单{}条, 导入未删除支付单{}条, 导入支付单错误{}条", fileNum, notDeleteNum, errorNum);

            if (response.getCount() < response.getPageSize()) {
                break;
            }
            requestPage.pagingNext();
        }
    }

    private void fileBill(Integer days, Integer areaId) {
        Date date = new Date();
        date = DateUtils.addDays(date, -days);
        date = DateUtils.setDays(date, 1);
        ApiRequest apiRequest = ApiRequest.newInstance().filterLessThan(QBppBill.receivableDate, date).filterEqual(QBppBill.areaId, areaId);
        ApiRequestPage requestPage = ApiRequestPage.newInstance().paging(0, 100);
        Integer errorNum = 0;
        Integer fileNum = 0;
        Integer notDeleteNum = 0;
        while (true) {
            ApiResponse<BppBill> response = bppBillApiService.findAll(apiRequest, requestPage);

            if (response == null || response.getCount() == 0) {
                break;
            }
            for (BppBill bppBill : response.getPagedData()) {
                try {
                    BppFee bppFee = bppFeeApiService.getFee(bppBill.getFeeId());
                    BppFeeScale bppFeeScale = bppFeeApiService.getFeeScale(bppBill.getScaleId());
                    List<HouseInfoIndex> houseInfoIndices = proHouseInfoIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseInfoIndex.addressId, bppBill.getAddressId()));
                    BppBillIndex bppBillIndex = BeanMapping.map(bppBill, BppBillIndex.class);
                    AreaInfo areaInfo = proAreaInfoApiService.findOne(bppBill.getAreaId());
                    if (bppFee != null) {
                        bppBillIndex.setFeeName(bppFee.getName());
                    }
                    if (bppFeeScale != null) {
                        bppBillIndex.setFeeScaleName(bppFeeScale.getName());
                    }
                    if (!CollectionUtils.isEmpty(houseInfoIndices)) {
                        bppBillIndex.setAddress(houseInfoIndices.get(0).getRoomAddress());
                    }
                    if (areaInfo != null) {
                        bppBillIndex.setAreaName(areaInfo.getAreaName());
                    }
                    if (bppBillIndex.getStatus().equals(BillStatus.RECEIVED)) {
                        BppOrder bppOrder = bppOrderApiService.getByBillId(bppBill.getId());
                        if (bppOrder != null) {
                            bppBillIndex.setPaidDate(bppOrder.getPaidAt().getTime());
                            if (bppBillIndex.getPaidDate() > bppBillIndex.getReceivableDate()) {
                                bppBillIndex.setBillPaidType(BillPaidType.OVERDUE);
                            } else {
                                bppBillIndex.setBillPaidType(BillPaidType.RECEIVED);
                            }
                        } else {
                            List<BppOrderDetailIndex> bppOrderDetailIndices = bppOrderIndexApiService.findDetail(ApiRequest.newInstance().filterEqual(QBppOrderDetailIndex.billId, bppBillIndex.getId()));
                            if (!CollectionUtils.isEmpty(bppOrderDetailIndices)) {
                                BppOrderIndex bppOrderIndex = bppOrderIndexApiService.getOrder(bppOrderDetailIndices.get(0).getOrderId());
                                if (bppOrderIndex != null) {
                                    if (bppBillIndex.getPaidDate() > bppBillIndex.getReceivableDate()) {
                                        bppBillIndex.setBillPaidType(BillPaidType.OVERDUE);
                                    } else {
                                        bppBillIndex.setBillPaidType(BillPaidType.RECEIVED);
                                    }
                                } else {
                                    bppBillIndex.setBillPaidType(BillPaidType.RECEIVED);
                                }
                            } else {
                                bppBillIndex.setBillPaidType(BillPaidType.RECEIVED);
                            }

                        }
                    } else {
                        bppBillIndex.setPaidDate(0L);
                        if (bppBillIndex.getStatus().equals(BillStatus.UNRECEIVE)) {
                            bppBillIndex.setBillPaidType(BillPaidType.UNRECEIVE);
                        } else {
                            bppBillIndex.setBillPaidType(BillPaidType.UNRECEIVE);
                        }
                    }
                    List<BppBillReduce> bppBillReduces = bppBillApiService.findAllReduce(bppBill.getId());
                    List<BppBillReduceIndex> bppBillReduceIndices = Lists.newArrayList();
                    if (!CollectionUtils.isEmpty(bppBillReduces)) {
                        bppBillReduceIndices = BeanMapping.mapList(bppBillReduces, BppBillReduceIndex.class);
                        Set<String> userOpenIdSet = Sets.newHashSet();
                        bppBillReduces.forEach(bppBillReduce -> {
                            userOpenIdSet.add(bppBillReduce.getOperationUser());
                            if (StringUtils.isNotEmpty(bppBillReduce.getCancelUser())) {
                                userOpenIdSet.add(bppBillReduce.getCancelUser());
                            }
                        });
                        List<Oauth2AccountIndex> oauth2AccountIndexList = businessUserAccountIndexApiService.findAll(ApiRequest.newInstance().filterEqual(QOauth2Account.clientId, "sqbj-smart").filterIn(QOauth2Account.userOpenId, userOpenIdSet));
                        Map<String, UserAccountIndex> userAccountIndexMap = Maps.newHashMap();
                        if (!CollectionUtils.isEmpty(oauth2AccountIndexList)) {
                            List<UserAccountIndex> userAccountIndices = businessUserAccountIndexApiService.findAccountAll(ApiRequest.newInstance().filterIn(QUserAccountIndex.id, oauth2AccountIndexList.stream().map(Oauth2AccountIndex::getAccountId).collect(Collectors.toList())));
                            Map<String, UserAccountIndex> map = Maps.newHashMap();
                            if (!CollectionUtils.isEmpty(userAccountIndices)) {
                                map = userAccountIndices.stream().collect(Collectors.toMap(UserAccountIndex::getId, userAccountIndex -> userAccountIndex));
                            }
                            for (Oauth2AccountIndex oauth2AccountIndex : oauth2AccountIndexList) {
                                UserAccountIndex userAccountIndex = map.get(oauth2AccountIndex.getAccountId());
                                if (userAccountIndex != null) {
                                    userAccountIndexMap.put(oauth2AccountIndex.getUserOpenId(), userAccountIndex);
                                }
                            }
                        }
                        for (BppBillReduceIndex bppBillReduceIndex : bppBillReduceIndices) {
                            UserAccountIndex operationUser = userAccountIndexMap.get(bppBillReduceIndex.getOperationUser());
                            if (operationUser != null) {
                                bppBillReduceIndex.setOperationUserName(operationUser.getRealName());
                                bppBillReduceIndex.setOperationPhone(operationUser.getPhone());
                            }
                            if (StringUtils.isNotEmpty(bppBillReduceIndex.getCancelUser())) {
                                UserAccountIndex cancelUser = userAccountIndexMap.get(bppBillReduceIndex.getCancelUser());
                                if (cancelUser != null) {
                                    bppBillReduceIndex.setCancelUserName(cancelUser.getRealName());
                                    bppBillReduceIndex.setCancelUserPhone(cancelUser.getPhone());
                                }
                            }
                        }
                    }
                    if (bppBill.getStatus().equals(BillStatus.UNRECEIVE)) {
                        bppBillIndexApiService.create(bppBillIndex, bppBillReduceIndices, false);
                        notDeleteNum++;
                    } else {
                        bppBillIndexApiService.create(bppBillIndex, bppBillReduceIndices, true);
                        fileNum++;
                    }
                } catch (Exception e) {
                    logger.error("处理归档账单错误, billId = {}", bppBill.getId(), e);
                    errorNum++;
                }
            }
            logger.error("正常导入账单{}条, 导入未删除账单{}条, 导入账单单错误{}条", fileNum, notDeleteNum, errorNum);

            if (response.getCount() < response.getPageSize()) {
                break;
            }
            requestPage.pagingNext();
        }
    }
}
