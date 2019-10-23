package cn.lehome.dispatcher.queue.listener.bill;

import cn.lehome.base.api.bpp.bean.bill.PreBppBill;
import cn.lehome.base.api.bpp.bean.bill.PreBuildBppBill;
import cn.lehome.base.api.bpp.bean.fee.*;
import cn.lehome.base.api.bpp.service.bill.PreBppBillApiService;
import cn.lehome.base.api.bpp.service.fee.BppFeeApiService;
import cn.lehome.base.api.bpp.util.RandomIdentifiesUtils;
import cn.lehome.base.pro.api.bean.area.AreaInfo;
import cn.lehome.base.pro.api.bean.house.HouseInfoIndex;
import cn.lehome.base.pro.api.bean.house.QHouseInfoIndex;
import cn.lehome.base.pro.api.bean.house.layout.ApartmentLayout;
import cn.lehome.base.pro.api.bean.house.layout.ApartmentMultiAcreage;
import cn.lehome.base.pro.api.service.area.AreaInfoApiService;
import cn.lehome.base.pro.api.service.house.ApartmentLayoutApiService;
import cn.lehome.base.pro.api.service.house.HouseInfoIndexApiService;
import cn.lehome.bean.bpp.enums.bill.BillStatus;
import cn.lehome.bean.bpp.enums.fee.*;
import cn.lehome.dispatcher.queue.bean.bill.BppBillScale;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2018/3/15.
 */
public class PreBuildBppBillListener extends AbstractJobListener {

    @Autowired
    private BppFeeApiService bppFeeApiService;

    @Autowired
    private PreBppBillApiService preBppBillApiService;

    @Autowired
    private HouseInfoIndexApiService smartHouseInfoApiService;

    @Autowired
    private ApartmentLayoutApiService apartmentLayoutApiService;

    @Autowired
    private AreaInfoApiService smartAreaInfoApiService;


    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        PreBuildBppBill preBuildBppBill = preBppBillApiService.get(longEventMessage.getData().intValue());
        if (preBuildBppBill == null) {
            logger.error("未找到预生成任务, id = " + longEventMessage.getData());
            return;
        }
        try {
            List<Integer> addressIds = JSON.parseArray(preBuildBppBill.getAddressIds(), Integer.class);
            List<BppBillScale> bppBillScales = JSON.parseArray(preBuildBppBill.getScaleTime(), BppBillScale.class);
            if (CollectionUtils.isEmpty(addressIds)) {
                logger.error("没有地址集合, addressIds = " + preBuildBppBill.getAddressIds());
                preBppBillApiService.deletePreBill(longEventMessage.getData().intValue());
                return;
            }
            AreaInfo areaInfo = smartAreaInfoApiService.findOne(preBuildBppBill.getAreaId());
            if (areaInfo == null) {
                logger.error("小区信息未找到, areaId = {}", preBuildBppBill.getAreaId());
                preBppBillApiService.deletePreBill(longEventMessage.getData().intValue());
                return;
            }
            if (CollectionUtils.isEmpty(bppBillScales)) {
                logger.error("没有费标设置, scaleTime = " + preBuildBppBill.getScaleTime());
                preBppBillApiService.deletePreBill(longEventMessage.getData().intValue());
                return;
            }
            Map<Integer, BppBillScale> bppBillScaleMap = bppBillScales.stream().collect(Collectors.toMap(BppBillScale::getFeeScaleId, bppBillScale -> bppBillScale));
            BppFee bppFee = bppFeeApiService.getFee(preBuildBppBill.getFeeId());
            if (bppFee == null) {
                logger.error("费项信息未找到, feeId = " + preBuildBppBill.getFeeId());
                preBppBillApiService.deletePreBill(longEventMessage.getData().intValue());
                return;
            }
            DefaultBppFeeModify defaultBppFeeModify = bppFeeApiService.findByTenantIdAndFeeId(areaInfo.getUniqueCode(), bppFee.getId());
            if (defaultBppFeeModify != null) {
                bppFee.setBillCycle(defaultBppFeeModify.getBillCycle());
                bppFee.setChargeObjectType(defaultBppFeeModify.getChargeObjectType());
            }
            Map<Integer, BppFeeScale> feeScaleMap = bppFeeApiService.findScaleAll(bppBillScales.stream().map(BppBillScale::getFeeScaleId).collect(Collectors.toList()));
            if (CollectionUtils.isEmpty(feeScaleMap) || feeScaleMap.size() != bppBillScales.size()) {
                logger.error("费标信息未找到, scaleIds = " + StringUtils.join(bppBillScales.stream().map(BppBillScale::getFeeScaleId).collect(Collectors.toList()), ","));
                preBppBillApiService.deletePreBill(longEventMessage.getData().intValue());
                return;
            }
            List<BppRefScaleAddress> bppRefScaleAddressList = bppFeeApiService.findScaleAddressAll(ApiRequest.newInstance().filterEqual(QBppRefScaleAddress.feeId, preBuildBppBill.getFeeId()).filterIn(QBppRefScaleAddress.addressId, addressIds));
            if (CollectionUtils.isEmpty(bppRefScaleAddressList)) {
                logger.error("费标与房子关系信息未找到, feeId = " + preBuildBppBill.getFeeId());
                preBppBillApiService.deletePreBill(longEventMessage.getData().intValue());
                return;
            }
            Map<Integer, BppRefScaleAddress> bppRefScaleAddressMap = bppRefScaleAddressList.stream().collect(Collectors.toMap(BppRefScaleAddress::getAddressId, bppRefScaleAddress -> bppRefScaleAddress));
            if (bppRefScaleAddressMap.size() < addressIds.size()) {
                logger.error("费标与房子关系信息未找到, size = {}, addressId.size = {}", bppRefScaleAddressMap.size(), addressIds.size());
                preBppBillApiService.deletePreBill(longEventMessage.getData().intValue());
                return;
            }

            for (Integer addressId : addressIds) {
                BppRefScaleAddress bppRefScaleAddress = bppRefScaleAddressMap.get(addressId);
                BppFeeScale bppFeeScale = feeScaleMap.get(bppRefScaleAddress.getScaleId());
                BppBillScale bppBillScale = bppBillScaleMap.get(bppFeeScale.getId());
                Date startTime = bppRefScaleAddress.getStartDate();
                BppFeeScaleCycle bppFeeScaleCycle = bppFeeApiService.findByScaleId(bppFeeScale.getId());
                List<Date> startTimes = getStartTimes(startTime, bppFeeScale.getChargeCycle(), bppBillScale.getEndTime(), bppFeeScaleCycle);
                if ((bppFee.getBillCycle().equals(BillCycle.MONTH) && bppFeeScale.getChargeCycle().equals(ChargeCycle.MONTH)) ||
                        (bppFee.getBillCycle().equals(BillCycle.YEAR) && bppFeeScale.getChargeCycle().equals(ChargeCycle.YEAR))) {
                    List<PreBppBill> list = isSameCycle(bppFeeScale, startTimes, addressId, preBuildBppBill, bppFee, areaInfo);
                    preBppBillApiService.batchPreBill(preBuildBppBill.getId(), list);
                } else {
                    List<PreBppBill> list = isNotCycle(bppFeeScale, startTimes, addressId, preBuildBppBill, bppFee, areaInfo);
                    preBppBillApiService.batchPreBill(preBuildBppBill.getId(), list);
                }
            }
        } catch (Exception e) {
            logger.error("预导入账单任务失败 : ", e);
            preBppBillApiService.deletePreBill(preBuildBppBill.getId());
        }


    }

    private List<PreBppBill> isNotCycle(BppFeeScale bppFeeScale, List<Date> startTimes, Integer addressId, PreBuildBppBill preBuildBppBill, BppFee bppFee, AreaInfo areaInfo) {
        BigDecimal amount = bppFeeScale.getChargeUnitPrice();
        if (!bppFeeScale.getCostingObjectType().equals(CostingObjectType.NONE)) {
            List<HouseInfoIndex> houseInfoIndexList = smartHouseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseInfoIndex.addressId, addressId));
            if (CollectionUtils.isEmpty(houseInfoIndexList)) {
                return Lists.newArrayList();
            }
            HouseInfoIndex houseInfoIndex = houseInfoIndexList.get(0);
            if (bppFeeScale.getCostingObjectType().equals(CostingObjectType.HOUSE)) {
                amount = amount.multiply(new BigDecimal(houseInfoIndex.getAcreage()));
            } else {
                if (houseInfoIndex.getLayoutMultiAcreageId() != null && houseInfoIndex.getLayoutMultiAcreageId() != 0) {
                    ApartmentMultiAcreage apartmentMultiAcreage = apartmentLayoutApiService.getAcreage(houseInfoIndex.getLayoutMultiAcreageId());
                    amount = amount.multiply(new BigDecimal(apartmentMultiAcreage.getAcreage()));
                } else {
                    ApartmentLayout apartmentLayout = apartmentLayoutApiService.get(houseInfoIndex.getLayoutId().longValue());
                    amount = amount.multiply(new BigDecimal(apartmentLayout.getAcreage()));
                }
            }
        }
        if (bppFeeScale.getChargeUnitTimeCycle().equals(ChargeUnitTimeCycle.MONTH)) {
            amount = amount.multiply(new BigDecimal(12));
        }
        int roundMode = BigDecimal.ROUND_HALF_UP;
        if (bppFeeScale.getOperateMode().equals(OperateMode.ROUND_DOWN)) {
            roundMode = BigDecimal.ROUND_DOWN;
        }
        amount = amount.setScale(bppFeeScale.getKeepFigures(), roundMode);
        BigDecimal perAmount = amount.divide(new BigDecimal(12), MathContext.DECIMAL128).setScale(bppFeeScale.getKeepFigures(), BigDecimal.ROUND_DOWN);
        BigDecimal specialAmount = perAmount.add(amount.subtract(perAmount.multiply(new BigDecimal(12))));
        Map<Integer, BigDecimal> bigDecimalMap = Maps.newConcurrentMap();
        for (Integer month = 1; month <= 12; month++) {
            bigDecimalMap.put(month, perAmount);
        }
        BppFeeScaleCycle bppFeeScaleCycle = null;
        if (bppFeeScale.getBillCycleSettingType().equals(BillCycleSettingType.NATURAL_YEAR)) {
            if (bppFeeScale.getSplitType().equals(SplitType.LAST_MONTH)) {
                bigDecimalMap.put(12, specialAmount);
            } else {
                bigDecimalMap.put(1, specialAmount);
            }
        } else {
            bppFeeScaleCycle = bppFeeApiService.findByScaleId(bppFeeScale.getId());
            if (bppFeeScale.getSplitType().equals(SplitType.LAST_MONTH)) {
                bigDecimalMap.put(bppFeeScaleCycle.getEndMonth(), specialAmount);
            } else {
                bigDecimalMap.put(bppFeeScaleCycle.getStartMonth(), specialAmount);
            }
        }
        List<PreBppBill> list = Lists.newArrayList();
        String batchCode = RandomIdentifiesUtils.getBillBatchCode();
        for (Date startTime : startTimes) {
            String name = DateUtils.toCalendar(startTime).get(Calendar.YEAR) + "年" + (DateUtils.toCalendar(startTime).get(Calendar.MONTH) + 1) + "月";;
            Date endTime = DateUtils.addDays(DateUtils.addMonths(startTime, 1), -1);
            Integer startDay = DateUtils.toCalendar(startTime).get(Calendar.DAY_OF_MONTH);
            if (startDay != 1) {
                endTime = DateUtils.setDays(endTime, 1);
                endTime = DateUtils.addDays(endTime, -1);
            }

            Date billTime = new Date();
            if (bppFeeScale.getBillCycleSettingType().equals(BillCycleSettingType.NATURAL_YEAR)) {
                billTime = DateUtils.setYears(billTime, DateUtils.toCalendar(startTime).get(Calendar.YEAR));
                billTime = DateUtils.setMonths(billTime, 11);
                billTime = DateUtils.setDays(billTime, 31);
                billTime = DateUtils.setHours(billTime, 23);
                billTime = DateUtils.setMinutes(billTime, 59);
                billTime = DateUtils.setSeconds(billTime, 59);
                billTime = DateUtils.setMilliseconds(billTime, 0);
            } else {
                if (bppFeeScaleCycle == null) {
                    billTime = DateUtils.setYears(billTime, DateUtils.toCalendar(startTime).get(Calendar.YEAR));
                    billTime = DateUtils.setMonths(billTime, 11);
                    billTime = DateUtils.setDays(billTime, 31);
                    billTime = DateUtils.setHours(billTime, 23);
                    billTime = DateUtils.setMinutes(billTime, 59);
                    billTime = DateUtils.setSeconds(billTime, 59);
                    billTime = DateUtils.setMilliseconds(billTime, 0);
                } else {
                    Integer currentStartMonth = DateUtils.toCalendar(startTime).get(Calendar.MONTH) + 1;
                    if (currentStartMonth < bppFeeScaleCycle.getStartMonth()) {
                        billTime = DateUtils.setYears(billTime, DateUtils.toCalendar(startTime).get(Calendar.YEAR));
                    } else {
                        billTime = DateUtils.setYears(billTime, DateUtils.toCalendar(startTime).get(Calendar.YEAR) + 1);
                    }
                    billTime = DateUtils.setMonths(billTime, bppFeeScaleCycle.getEndMonth());
                    billTime = DateUtils.setDays(billTime, 1);
                    billTime = DateUtils.setHours(billTime, 23);
                    billTime = DateUtils.setMinutes(billTime, 59);
                    billTime = DateUtils.setSeconds(billTime, 59);
                    billTime = DateUtils.setMilliseconds(billTime, 0);
                    billTime = DateUtils.addDays(billTime, -1);
                }
            }
            BigDecimal payAmount = bigDecimalMap.get(DateUtils.toCalendar(startTime).get(Calendar.MONTH) + 1);
            name += bppFee.getName();
            PreBppBill preBppBill = new PreBppBill();
            preBppBill.setAreaId(preBuildBppBill.getAreaId());
            preBppBill.setStartDate(startTime);
            preBppBill.setAddressId(addressId);
            preBppBill.setBatchCode(batchCode);
            preBppBill.setBillNumber(RandomIdentifiesUtils.getBillNumber(preBuildBppBill.getId(), startTime, bppFeeScale.getId()));
            preBppBill.setDiscountAmount(BigDecimal.ZERO);
            preBppBill.setEndDate(endTime);
            preBppBill.setFeeId(bppFeeScale.getFeeId());
            preBppBill.setName(name);
            preBppBill.setPaidAmount(payAmount);
            preBppBill.setPayableAmount(payAmount);
            preBppBill.setPreBuildId(preBuildBppBill.getId());
            preBppBill.setReceivableDate(billTime);
            preBppBill.setScaleId(bppFeeScale.getId());
            preBppBill.setStatus(BillStatus.UNRECEIVE);
            preBppBill.setTenantCode(areaInfo.getUniqueCode());
            BigDecimal reDiscountAmount = BigDecimal.ZERO;
            if (startDay != 1) {
                if (bppFeeScale.getChargeCycle().equals(ChargeCycle.MONTH)) {
                    reDiscountAmount = preBppBill.getPaidAmount().divide(new BigDecimal(30), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(startDay)).setScale(bppFeeScale.getKeepFigures(), roundMode);
                } else {
                    Integer startMonth = DateUtils.toCalendar(preBppBill.getStartDate()).get(Calendar.MONTH) + 1;
                    reDiscountAmount = preBppBill.getPaidAmount().divide(new BigDecimal(12), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(startMonth)).setScale(bppFeeScale.getKeepFigures(), roundMode);
                }
            }
            preBppBill.setReDiscountAmount(reDiscountAmount);
            list.add(preBppBill);
        }
        return list;
    }

    private List<PreBppBill> isSameCycle(BppFeeScale bppFeeScale, List<Date> startTimes, Integer addressId, PreBuildBppBill preBuildBppBill, BppFee bppFee, AreaInfo areaInfo) {
        BigDecimal amount = bppFeeScale.getChargeUnitPrice();
        if (!bppFeeScale.getCostingObjectType().equals(CostingObjectType.NONE)) {
            List<HouseInfoIndex> houseInfoIndexList = smartHouseInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QHouseInfoIndex.addressId, addressId));
            if (CollectionUtils.isEmpty(houseInfoIndexList)) {
                return Lists.newArrayList();
            }
            HouseInfoIndex houseInfoIndex = houseInfoIndexList.get(0);
            if (bppFeeScale.getCostingObjectType().equals(CostingObjectType.HOUSE)) {
                amount = amount.multiply(new BigDecimal(houseInfoIndex.getAcreage()));
            } else {
                if (houseInfoIndex.getLayoutMultiAcreageId() != null && houseInfoIndex.getLayoutMultiAcreageId() != 0) {
                    ApartmentMultiAcreage apartmentMultiAcreage = apartmentLayoutApiService.getAcreage(houseInfoIndex.getLayoutMultiAcreageId());
                    amount = amount.multiply(new BigDecimal(apartmentMultiAcreage.getAcreage()));
                } else {
                    ApartmentLayout apartmentLayout = apartmentLayoutApiService.get(houseInfoIndex.getLayoutId().longValue());
                    amount = amount.multiply(new BigDecimal(apartmentLayout.getAcreage()));
                }
            }
        }
        if (bppFeeScale.getChargeCycle().equals(ChargeCycle.MONTH) && bppFeeScale.getChargeUnitTimeCycle().equals(ChargeUnitTimeCycle.YEAR)) {
            amount = amount.divide(new BigDecimal(12), MathContext.DECIMAL128);
        } else if (bppFeeScale.getChargeCycle().equals(ChargeCycle.YEAR) && bppFeeScale.getChargeUnitTimeCycle().equals(ChargeUnitTimeCycle.MONTH)) {
            amount = amount.multiply(new BigDecimal(12));
        }
        int roundMode = BigDecimal.ROUND_HALF_UP;
        if (bppFeeScale.getOperateMode().equals(OperateMode.ROUND_DOWN)) {
            roundMode = BigDecimal.ROUND_DOWN;
        }
        BppFeeScaleCycle bppFeeScaleCycle = bppFeeApiService.findByScaleId(bppFeeScale.getId());

        amount = amount.setScale(bppFeeScale.getKeepFigures(), roundMode);
        List<PreBppBill> list = Lists.newArrayList();
        String batchCode = RandomIdentifiesUtils.getBillBatchCode();
        for (Date startTime : startTimes) {
            String name = DateUtils.toCalendar(startTime).get(Calendar.YEAR) + "年";
            Date endTime = new Date(startTime.getTime());
            Date billTime = new Date(startTime.getTime());
            if (bppFeeScale.getChargeCycle().equals(ChargeCycle.MONTH)) {
                endTime = DateUtils.addMonths(endTime, 1);
                endTime = DateUtils.setDays(endTime, 1);
                endTime = DateUtils.addDays(endTime, -1);
                name += (DateUtils.toCalendar(startTime).get(Calendar.MONTH) + 1) + "月";
                billTime = new Date(endTime.getTime());
            } else {
                if (bppFeeScale.getBillCycleSettingType().equals(BillCycleSettingType.NATURAL_YEAR)) {
                    endTime = DateUtils.addYears(endTime, 1);
                    endTime = DateUtils.setMonths(endTime, 0);
                    endTime = DateUtils.setDays(endTime, 1);
                    endTime = DateUtils.addDays(endTime, -1);
                } else {
                    if (bppFeeScaleCycle == null) {
                        endTime = DateUtils.addYears(endTime, 1);
                        endTime = DateUtils.setMonths(endTime, 0);
                        endTime = DateUtils.setDays(endTime, 1);
                        endTime = DateUtils.addDays(endTime, -1);
                    } else {
                        int startMonth = DateUtils.toCalendar(startTime).get(Calendar.MONTH) + 1;
                        if (startMonth == 1) {
                            startTime = DateUtils.setMonths(startTime, bppFeeScaleCycle.getStartMonth() - 1);
                            startTime = DateUtils.setDays(startTime, 1);
                        }
                        startMonth = DateUtils.toCalendar(startTime).get(Calendar.MONTH) + 1;
                        if (startMonth >= bppFeeScaleCycle.getStartMonth()) {
                            endTime = DateUtils.addYears(endTime, 1);
                            endTime = DateUtils.setMonths(endTime, bppFeeScaleCycle.getEndMonth() - 1);
                            endTime = DateUtils.setDays(endTime, 1);
                            endTime = DateUtils.addMonths(endTime, 1);
                            endTime = DateUtils.addDays(endTime, -1);
                        } else {
                            endTime = DateUtils.setMonths(endTime, bppFeeScaleCycle.getEndMonth() - 1);
                            endTime = DateUtils.setDays(endTime, 1);
                            endTime = DateUtils.addMonths(endTime, 1);
                            endTime = DateUtils.addDays(endTime, -1);
                        }
                    }
                }
                billTime = new Date(endTime.getTime());
            }
            name += bppFee.getName();
            PreBppBill preBppBill = new PreBppBill();
            preBppBill.setAreaId(preBuildBppBill.getAreaId());
            preBppBill.setStartDate(startTime);
            preBppBill.setAddressId(addressId);
            preBppBill.setBatchCode(batchCode);
            preBppBill.setBillNumber(RandomIdentifiesUtils.getBillNumber(preBuildBppBill.getId(), startTime, bppFeeScale.getId()));
            preBppBill.setDiscountAmount(BigDecimal.ZERO);
            preBppBill.setEndDate(endTime);
            preBppBill.setFeeId(bppFeeScale.getFeeId());
            preBppBill.setName(name);
            preBppBill.setPaidAmount(amount);
            preBppBill.setPayableAmount(amount);
            preBppBill.setPreBuildId(preBuildBppBill.getId());
            preBppBill.setReDiscountAmount(BigDecimal.ZERO);
            preBppBill.setReceivableDate(billTime);
            preBppBill.setScaleId(bppFeeScale.getId());
            preBppBill.setStatus(BillStatus.UNRECEIVE);
            preBppBill.setTenantCode(areaInfo.getUniqueCode());
            Integer startDay = DateUtils.toCalendar(preBppBill.getStartDate()).get(Calendar.DAY_OF_MONTH);
            BigDecimal reDiscountAmount = BigDecimal.ZERO;
            if (startDay != 1) {
                if (bppFeeScale.getChargeCycle().equals(ChargeCycle.MONTH)) {
                    reDiscountAmount = preBppBill.getPaidAmount().divide(new BigDecimal(30), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(startDay - 1)).setScale(bppFeeScale.getKeepFigures(), roundMode);
                } else {
                    Integer startMonth = DateUtils.toCalendar(preBppBill.getStartDate()).get(Calendar.MONTH) + 1;
                    if (bppFeeScaleCycle != null) {
                        if (startMonth >= bppFeeScaleCycle.getStartMonth()) {
                            startMonth = startMonth - bppFeeScaleCycle.getStartMonth();
                        } else {
                            startMonth = bppFeeScaleCycle.getEndMonth() - startMonth;
                        }
                    }
                    reDiscountAmount = preBppBill.getPaidAmount().divide(new BigDecimal(12), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(startMonth)).setScale(bppFeeScale.getKeepFigures(), roundMode);
                }
            } else {
                Integer startMonth = DateUtils.toCalendar(preBppBill.getStartDate()).get(Calendar.MONTH) + 1;
                if ((bppFeeScaleCycle == null && startMonth != 1) ) {
                    reDiscountAmount = preBppBill.getPaidAmount().divide(new BigDecimal(12), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(startMonth - 1)).setScale(bppFeeScale.getKeepFigures(), roundMode);
                } else if (bppFeeScaleCycle != null && startMonth != bppFeeScaleCycle.getStartMonth()) {
                    if (startMonth >= bppFeeScaleCycle.getStartMonth()) {
                        startMonth = startMonth - bppFeeScaleCycle.getStartMonth();
                    } else {
                        startMonth = 12 - bppFeeScaleCycle.getStartMonth() + startMonth - 1;
                    }
                    reDiscountAmount = preBppBill.getPaidAmount().divide(new BigDecimal(12), 4, BigDecimal.ROUND_HALF_UP).multiply(new BigDecimal(startMonth)).setScale(bppFeeScale.getKeepFigures(), roundMode);
                }
            }
            preBppBill.setReDiscountAmount(reDiscountAmount);
            list.add(preBppBill);
        }
        return list;

    }

//    private Date getStartTime(Date startTime, BppFeeScale bppFeeScale) {
//        if (bppFeeScale.getChargeCycle().equals(ChargeCycle.MONTH)) {
//            return DateUtils.setDays(startTime, 1);
//        } else if (bppFeeScale.getChargeCycle().equals(ChargeCycle.YEAR)) {
//            if (bppFeeScale.getBillCycleSettingType().equals(BillCycleSettingType.NATURAL_YEAR)) {
//                startTime = DateUtils.setMonths(startTime, 0);
//                startTime = DateUtils.setDays(startTime, 1);
//                return startTime;
//            } else if (bppFeeScale.getBillCycleSettingType().equals(BillCycleSettingType.UNNATURAL_YEAR)) {
//                BppFeeScaleCycle bppFeeScaleCycle = bppFeeApiService.findByScaleId(bppFeeScale.getId());
//                startTime = DateUtils.setMonths(startTime, bppFeeScaleCycle.getStartMonth() - 1);
//                startTime = DateUtils.setDays(startTime, 1);
//                return startTime;
//            }
//        }
//        return startTime;
//    }

    private List<Date> getStartTimes(Date startTime, ChargeCycle chargeCycle, Integer endTime, BppFeeScaleCycle bppFeeScaleCycle) {
        List<Date> list = Lists.newArrayList();
        while (getTime(startTime, chargeCycle) <= endTime) {
            list.add(startTime);
            if (chargeCycle.equals(ChargeCycle.MONTH)) {
                startTime = DateUtils.addMonths(startTime, 1);
                startTime = DateUtils.setDays(startTime, 1);
            } else {
                if (bppFeeScaleCycle != null) {
                    int startMonth = DateUtils.toCalendar(startTime).get(Calendar.MONTH) + 1;
                    if (bppFeeScaleCycle.getStartMonth() != startMonth) {
                        if (startMonth >= bppFeeScaleCycle.getStartMonth()) {
                            startTime = DateUtils.addYears(startTime, 1);
                            startTime = DateUtils.setMonths(startTime, bppFeeScaleCycle.getStartMonth() - 1);
                            startTime = DateUtils.setDays(startTime, 1);
                        } else {
                            startTime = DateUtils.setMonths(startTime, bppFeeScaleCycle.getStartMonth() - 1);
                            startTime = DateUtils.setDays(startTime, 1);
                        }
                    } else {
                        startTime = DateUtils.addYears(startTime, 1);
                    }
                } else {
                    startTime = DateUtils.addYears(startTime, 1);
                    startTime = DateUtils.setMonths(startTime, 0);
                    startTime = DateUtils.setDays(startTime, 1);
                }
            }
        }
        return list;
    }

    private Integer getTime(Date startTime, ChargeCycle chargeCycle) {
        if (chargeCycle.equals(ChargeCycle.MONTH)) {
            Integer month = DateUtils.toCalendar(startTime).get(Calendar.MONTH) + 1;
            String monthStr = month >= 10 ? month + "" : "0" + month;
            return Integer.valueOf(DateUtils.toCalendar(startTime).get(Calendar.YEAR) + monthStr);
        } else {
            return DateUtils.toCalendar(startTime).get(Calendar.YEAR);
        }
    }

    @Override
    public String getConsumerId() {
        return "pre_build_bpp";
    }


}
