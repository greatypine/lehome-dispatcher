package cn.lehome.dispatcher.queue.listener.areaImport;

import cn.lehome.base.api.acs.bean.user.User;
import cn.lehome.base.api.acs.service.user.UserApiService;
import cn.lehome.base.api.bpp.bean.bill.BppBill;
import cn.lehome.base.api.bpp.bean.bill.QBppBill;
import cn.lehome.base.api.bpp.bean.fee.*;
import cn.lehome.base.api.bpp.bean.order.BppOrder;
import cn.lehome.base.api.bpp.bean.order.BppOrderDetail;
import cn.lehome.base.api.bpp.bean.order.QBppOrder;
import cn.lehome.base.api.bpp.service.bill.BppBillApiService;
import cn.lehome.base.api.bpp.service.fee.BppFeeApiService;
import cn.lehome.base.api.bpp.service.order.BppOrderApiService;
import cn.lehome.base.api.common.business.oauth2.bean.sys.QSysRole;
import cn.lehome.base.api.common.business.oauth2.bean.sys.SysRole;
import cn.lehome.base.api.common.business.oauth2.bean.sys.SysUsersRoles;
import cn.lehome.base.api.common.business.oauth2.bean.user.Oauth2Account;
import cn.lehome.base.api.common.business.oauth2.service.sys.SysRoleApiService;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.base.api.common.custom.oauth2.bean.user.UserAccount;
import cn.lehome.base.api.common.custom.oauth2.service.user.UserAccountApiService;
import cn.lehome.base.api.common.operation.bean.application.Applications;
import cn.lehome.base.api.common.operation.bean.application.ApplicationsTenant;
import cn.lehome.base.api.common.operation.bean.application.QApplicationsTenant;
import cn.lehome.base.api.common.operation.bean.role.QRoleMapper;
import cn.lehome.base.api.common.operation.bean.role.RoleMapper;
import cn.lehome.base.api.common.operation.service.application.ApplicationApiService;
import cn.lehome.base.api.common.operation.service.application.ApplicationsTenantApiService;
import cn.lehome.base.api.common.operation.service.role.RoleTemplateApiService;
import cn.lehome.base.pro.api.bean.address.AddressBaseInfo;
import cn.lehome.base.pro.api.bean.area.AreaInfo;
import cn.lehome.base.pro.api.bean.area.ImportTask;
import cn.lehome.base.pro.api.bean.event.ImportDataPartConstants;
import cn.lehome.base.pro.api.bean.event.ImportEventBean;
import cn.lehome.base.pro.api.bean.house.AddressBean;
import cn.lehome.base.pro.api.bean.house.HouseInfo;
import cn.lehome.base.pro.api.bean.house.QHouseInfo;
import cn.lehome.base.pro.api.bean.households.HouseholdsUser;
import cn.lehome.base.pro.api.bean.households.settings.Household;
import cn.lehome.base.pro.api.bean.households.settings.HouseholdsSettingsInfo;
import cn.lehome.base.pro.api.bean.households.settings.QHouseholdsSettingsInfo;
import cn.lehome.base.pro.api.service.address.AddressBaseApiService;
import cn.lehome.base.pro.api.service.area.AreaInfoApiService;
import cn.lehome.base.pro.api.service.area.ImportTaskApiService;
import cn.lehome.base.pro.api.service.house.HouseInfoApiService;
import cn.lehome.base.pro.api.service.households.HouseholdsApiService;
import cn.lehome.base.pro.api.service.households.HouseholdsInfoApiService;
import cn.lehome.base.pro.api.service.households.HouseholdsUserApiService;
import cn.lehome.base.smart.abac.bean.RoleUsers;
import cn.lehome.base.smart.abac.service.SmartAbacApiService;
import cn.lehome.base.smart.oauth2.bean.QSmartOauth2Account;
import cn.lehome.base.smart.oauth2.bean.QSmartUserAccount;
import cn.lehome.base.smart.oauth2.bean.SmartOauth2Account;
import cn.lehome.base.smart.oauth2.bean.SmartUserAccount;
import cn.lehome.base.smart.oauth2.service.SmartOauth2UserAccountApiService;
import cn.lehome.bean.acs.enums.user.UserType;
import cn.lehome.bean.bpp.enums.fee.*;
import cn.lehome.bean.pro.enums.EnabledStatus;
import cn.lehome.bean.pro.enums.Gender;
import cn.lehome.bean.pro.enums.RecordDeleteStatus;
import cn.lehome.bean.pro.enums.address.ExtendType;
import cn.lehome.bean.pro.enums.area.ImportTaskStatus;
import cn.lehome.common.bean.business.oauth2.enums.sys.RoleType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.bean.core.enums.AccountType;
import cn.lehome.framework.bean.core.enums.SexType;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2019/5/31.
 */
public class AreaFlushListener extends AbstractJobListener {

    @Autowired
    private ImportTaskApiService importTaskApiService;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Autowired
    private AreaInfoApiService smartAreaInfoApiService;

    @Autowired
    private HouseInfoApiService smartHouseInfoApiService;

    @Autowired
    private HouseholdsApiService smartHouseholdsApiService;

    @Autowired
    private HouseholdsInfoApiService smartHouseholdsInfoApiService;

    @Autowired
    private UserApiService userApiService;

    @Autowired
    private HouseholdsUserApiService householdsUserApiService;

    @Autowired
    private UserAccountApiService userAccountApiService;

    @Autowired
    private cn.lehome.base.api.common.business.oauth2.service.user.UserAccountApiService businessUserAccountApiService;

    @Autowired
    private SmartOauth2UserAccountApiService smartOauth2UserAccountApiService;

    @Autowired
    private AddressBaseApiService addressBaseApiService;

    @Autowired
    private SmartAbacApiService smartAbacApiService;

    private static final String BPP_KEY = "pbpp";

    private static final String externalClientId = "3";

    @Autowired
    private ApplicationApiService applicationApiService;

    @Autowired
    private ApplicationsTenantApiService applicationsTenantApiService;

    @Autowired
    private BppFeeApiService bppFeeApiService;

    @Autowired
    private BppBillApiService bppBillApiService;

    @Autowired
    private BppOrderApiService bppOrderApiService;

    @Autowired
    private SysRoleApiService sysRoleApiService;

    @Autowired
    private RoleTemplateApiService roleTemplateApiService;

    private static final Integer PAGE_SIZE = 30;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof SimpleEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<ImportEventBean> simpleEventMessage = (SimpleEventMessage<ImportEventBean>) eventMessage;
        ImportEventBean importEventBean = simpleEventMessage.getData();
        ImportTask importTask = importTaskApiService.get(importEventBean.getTaskId());
        if (importTask == null) {
            logger.error("导入任务未找到, id = " + importEventBean.getTaskId());
            return;
        }

        importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FLUSHING, "");

        switch (importEventBean.getPart()) {
            case ImportDataPartConstants.ALL:
            case ImportDataPartConstants.HOUSE:
                this.flushHouse(importEventBean.getStartId(), importTask.getAreaId(), importTask, importEventBean);
                break;
            case ImportDataPartConstants.HOUSEHOLD:
                this.flushHousehold(importEventBean.getStartId(), importTask.getAreaId(), importTask, importEventBean);
                break;
            case ImportDataPartConstants.FINANCE:
                this.flushFinance(importTask.getAreaId(), importTask, importEventBean);
                break;
            case ImportDataPartConstants.STAFF:
                this.flushStaff(importEventBean.getStartId(), importTask.getAreaId(), importTask, importEventBean);
                break;
        }
    }


    private void flushHouse(Integer startId, Integer areaId, ImportTask importTask, ImportEventBean importEventBean) {
        logger.info("开始刷新房产, areaId = {}, startId = {}", areaId, startId);
        ApiRequest apiRequest = ApiRequest.newInstance().filterEqual(QHouseInfo.areaId, areaId).filterGreaterThan(QHouseInfo.id, startId);
        ApiResponse<HouseInfo> response = smartHouseInfoApiService.findAll(apiRequest, ApiRequestPage.newInstance().paging(0, PAGE_SIZE).addOrder(QHouseInfo.id, PageOrderType.ASC));
        int num = 0;
        int delNum = 0;
        int lastId = 0;
        boolean isSucess = true;
        boolean isFirst = false;
        if (startId == 0) {
            isFirst = true;
        }
        if (!CollectionUtils.isEmpty(response.getPagedData())) {
            for (HouseInfo houseInfo : response.getPagedData()) {
                try {
                    if (houseInfo.getEnabledStatus().equals(EnabledStatus.Disabled)) {
                        delNum += 1;
                    } else {
                        AddressBaseInfo addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.HOUSE, houseInfo.getId().longValue());
                        if (addressBaseInfo != null) {
                            AddressBean addressBean = JSON.parseObject(addressBaseInfo.getAddress(), AddressBean.class);
                            houseInfo.setManagerAreaName(addressBean.getProjectName());
                            houseInfo.setAreaName(addressBean.getAreaName());
                            houseInfo.setFloorNo(addressBean.getFloorNumber());
                            houseInfo.setFloorName(addressBean.getFloorName());
                            houseInfo.setUnitNo(addressBean.getUnitNumber());
                            houseInfo.setUnitName(addressBean.getUnitName());
                            houseInfo.setRoomName(addressBean.getRoomName());
                            String roomAddress = houseInfo.getManagerAreaName();
                            if (StringUtils.isNotEmpty(houseInfo.getFloorNo())) {
                                roomAddress += "-" + houseInfo.getFloorNo();
                                if (StringUtils.isNotEmpty(houseInfo.getFloorName())) {
                                    roomAddress += houseInfo.getFloorName();
                                }
                            }
                            if (StringUtils.isNotEmpty(houseInfo.getUnitNo())) {
                                roomAddress += "-" + houseInfo.getUnitNo();
                                if (StringUtils.isNotEmpty(houseInfo.getUnitName())) {
                                    roomAddress += houseInfo.getUnitName();
                                }
                            }
                            roomAddress += "-" + houseInfo.getRoomId();
                            if (StringUtils.isNotEmpty(houseInfo.getRoomName())) {
                                roomAddress += houseInfo.getRoomName();
                            }
                            houseInfo.setRoomAddress(roomAddress);
                        }
                        smartHouseInfoApiService.update(houseInfo);
                        num += 1;
                    }
                    lastId = houseInfo.getId();
                } catch (Exception e) {
                    logger.error("刷新数据出错, id = " + houseInfo.getId(), e);
                    isSucess = false;
                    break;
                }
            }
        }

        if (!isFirst) {
            importTask.setHouseNum(importTask.getHouseNum() + num);
            importTask.setDelHouseNum(importTask.getDelHouseNum() + delNum);
        } else {
            importTask.setHouseNum(num);
            importTask.setDelHouseNum(delNum);
        }

        importTaskApiService.updateNum(importTask);
        if (!isSucess) {
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FAILED, "刷新房间错误");
            return;
        }


        if (response.getCount() == 0 || response.getCount() < PAGE_SIZE) {
            if (!importEventBean.isContinue()) {
                importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FINISHED, "");
            } else {
                importEventBean.setPart(ImportDataPartConstants.HOUSEHOLD);
                importEventBean.setStartId(0);
                eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.FLUSH_AREA_DATA_EVENT, importEventBean));
            }
        } else {
            importEventBean.setPart(ImportDataPartConstants.HOUSE);
            importEventBean.setStartId(lastId);
            eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.FLUSH_AREA_DATA_EVENT, importEventBean));
        }
    }

    private void flushHousehold(Integer startId, Integer areaId, ImportTask importTask, ImportEventBean importEventBean) {
        logger.info("开始刷新住户, areaId = {}, startId = {}", areaId, startId);
        ApiResponse<HouseholdsSettingsInfo> response = smartHouseholdsApiService.findSettingAll(ApiRequest.newInstance().filterEqual(QHouseholdsSettingsInfo.areaId, areaId).filterGreaterThan(QHouseholdsSettingsInfo.id, startId), ApiRequestPage.newInstance().paging(0, PAGE_SIZE).addOrder(QHouseholdsSettingsInfo.id, PageOrderType.ASC));
        AreaInfo areaInfo = smartAreaInfoApiService.findOne(areaId);
        if (areaInfo == null) {
            logger.error("小区信息未找到, id = " + areaId);
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FAILED, "刷新住户数据错误");
            return;
        }
        int num = 0;
        int delNum = 0;
        int lastId = 0;
        int acsNum = 0;
        boolean isSucess = true;
        boolean isFirst = false;
        if (startId == 0) {
            isFirst = true;
        }
        if (!CollectionUtils.isEmpty(response.getPagedData())) {
            for (HouseholdsSettingsInfo householdSettingsInfo : response.getPagedData()) {
                try {
                    if (householdSettingsInfo.getDeleteStatus().equals(RecordDeleteStatus.Deleted)) {
                        delNum += 1;
                    } else {
                        Household household = smartHouseholdsInfoApiService.findOne(householdSettingsInfo.getId().longValue());
                        if (household != null) {
                            if (StringUtils.isNotEmpty(household.getTelephone())) {
                                UserAccount userAccount = userAccountApiService.getByPhone(household.getTelephone());
                                if (userAccount == null) {
                                    SexType sexType;
                                    if (household.getGender() == Gender.Male) {
                                        sexType = SexType.Male;
                                    } else if (household.getGender() == Gender.Female) {
                                        sexType = SexType.Female;
                                    } else {
                                        sexType = SexType.Unknown;
                                    }
                                    userAccount = userAccountApiService.createBySmartPro(household.getTelephone(), household.getName(), sexType);
                                }
                                if (StringUtils.isNotEmpty(household.getTelephone())) {
                                    HouseholdsUser householdsUser = householdsUserApiService.findOne(household.getTelephone(), areaInfo.getUniqueCode());
                                    if (householdsUser == null) {
                                        householdsUser = new HouseholdsUser();
                                        householdsUser.setOpenId(household.getOpenId());
                                        householdsUser.setPhone(household.getTelephone());
                                        householdsUser.setTenantCode(areaInfo.getUniqueCode());
                                        householdsUserApiService.create(householdsUser);
                                    }
                                }

                                smartHouseholdsInfoApiService.updateUserId(householdSettingsInfo.getId(), userAccount.getId().intValue());
                                User user = userApiService.findByTraceId(UserType.Resident, household.getOpenId());
                                if (user == null) {
                                    user = new User();
                                    user.setTraceId(household.getOpenId());
                                    user.setUserType(UserType.Resident);
                                    user.setUserId(userAccount.getId());
                                    userApiService.create(user);
                                } else {
                                    user.setUserId(userAccount.getId());
                                    userApiService.updateUserId(user.getId(), userAccount.getId());
                                }
                            }
                        }
                        num += 1;
                        acsNum += 1;
                    }
                    lastId = householdSettingsInfo.getId();
                } catch (Exception e) {
                    logger.error("刷新住户数据出错, id = " + householdSettingsInfo.getId(), e);
                    isSucess = false;
                    break;
                }
            }
        }

        if (!isFirst) {
            importTask.setHouseholdNum(importTask.getHouseholdNum() + num);
            importTask.setDelHouseholdNum(importTask.getDelHouseholdNum() + delNum);
            importTask.setAcsUserNum(importTask.getAcsUserNum() + acsNum);
        } else {
            importTask.setHouseholdNum(num);
            importTask.setDelHouseholdNum(delNum);
            importTask.setAcsUserNum(acsNum);
        }

        importTaskApiService.updateNum(importTask);
        if (!isSucess) {
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FAILED, "刷新住户错误");
            return;
        }


        if (response.getCount() == 0 || response.getCount() < PAGE_SIZE) {
            if (!importEventBean.isContinue()) {
                importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FINISHED, "");
            } else {
                importEventBean.setPart(ImportDataPartConstants.FINANCE);
                importEventBean.setStartId(0);
                eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.FLUSH_AREA_DATA_EVENT, importEventBean));
            }
        } else {
            importEventBean.setPart(ImportDataPartConstants.HOUSEHOLD);
            importEventBean.setStartId(lastId);
            eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.FLUSH_AREA_DATA_EVENT, importEventBean));
        }
    }

    private void flushFinance(Integer areaId, ImportTask importTask, ImportEventBean importEventBean) {
        logger.info("开始财务数据, areaId = {}", areaId);
        List<Applications> applicationsList = applicationApiService.findByKey(Sets.newHashSet(BPP_KEY));
        if (CollectionUtils.isEmpty(applicationsList)) {
            logger.error("应用信息未找到");
            return;
        }
        List<ApplicationsTenant> applicationsTenants = applicationsTenantApiService.findAll(ApiRequest.newInstance().filterEqual(QApplicationsTenant.applicationId, applicationsList.get(0).getId()));
        if (CollectionUtils.isEmpty(applicationsTenants)) {
            logger.error("没有开通财务模块不需要刷新财务数据, areaId = " + areaId);
            importEventBean.setPart(ImportDataPartConstants.STAFF);
            importEventBean.setStartId(0);
            eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.FLUSH_AREA_DATA_EVENT, importEventBean));
            return;
        }
        int bppFeeScaleNum = 0;
        int bppBill = 0;
        int bppOrderNum = 0;
        boolean isSucess = true;
        try {
            List<BppFeeScale> bppFeeScaleList = bppFeeApiService.findScaleAll(ApiRequest.newInstance().filterEqual(QBppFeeScale.areaId, areaId).filterEqual(QBppFeeScale.deleteStatus, BppFeeApiService.NORMAL_STATUS));
            Map<Integer, BppFeeScale> firstScaleMap = Maps.newHashMap();
            ArrayListMultimap<Integer, BppFeeScale> scaleArrayList = ArrayListMultimap.create();
            for (BppFeeScale bppFeeScale : bppFeeScaleList) {
                if (bppFeeScale.getParentScaleId() == null || bppFeeScale.getParentScaleId() == 0) {
                    if (bppFeeScale.getChargeCycle().equals(ChargeCycle.YEAR)) {
                        bppFeeScale.setChargeUnitTimeCycle(ChargeUnitTimeCycle.YEAR);
                    } else if (bppFeeScale.getChargeCycle().equals(ChargeCycle.MONTH)) {
                        bppFeeScale.setChargeUnitTimeCycle(ChargeUnitTimeCycle.MONTH);
                    }
                    BppFee bppFee = bppFeeApiService.getFee(bppFeeScale.getFeeId());
                    if (bppFee.getBillCycle().equals(BillCycle.YEAR) && bppFeeScale.getChargeCycle().equals(ChargeCycle.MONTH)) {
                        bppFeeScale.setSplitType(SplitType.FIRST_MONTH);
                    }
                    if (bppFee.getBillCycle().equals(BillCycle.MONTH)) {
                        bppFeeScale.setBillCycleSettingType(BillCycleSettingType.NONE);
                    }
                    bppFeeApiService.updateFeeScale(bppFeeScale);
                    bppFeeScaleNum += 1;
                    firstScaleMap.put(bppFeeScale.getId(), bppFeeScale);
                } else {
                    scaleArrayList.put(bppFeeScale.getParentScaleId(), bppFeeScale);
                }
            }
            for (Integer scaleId : scaleArrayList.keySet()) {
                List<BppFeeScale> subBppFeeScaleList = scaleArrayList.get(scaleId);
                List<BppRefScaleAddress> list = Lists.newArrayList();
                List<Integer> billList = Lists.newArrayList();
                for (BppFeeScale bppFeeScale : subBppFeeScaleList) {
                    List<BppRefScaleAddress> bppRefScaleAddressList = bppFeeApiService.findScaleAddressAll(ApiRequest.newInstance().filterEqual(QBppRefScaleAddress.scaleId, bppFeeScale.getId()));
                    if (!CollectionUtils.isEmpty(bppRefScaleAddressList)) {
                        list.addAll(bppRefScaleAddressList);
                    }
                    List<BppBill> bppBillList = bppBillApiService.findAll(ApiRequest.newInstance().filterEqual(QBppBill.scaleId, bppFeeScale.getId()));
                    if (!CollectionUtils.isEmpty(bppBillList)) {
                        billList.addAll(bppBillList.stream().map(BppBill::getId).collect(Collectors.toList()));
                    }
                }
                if (!CollectionUtils.isEmpty(list)) {
                    bppFeeApiService.batchSaveOrUpdateAddress(scaleId, list);
                }
                if (!CollectionUtils.isEmpty(billList)) {
                    bppBillApiService.batchUpdateScale(billList, scaleId);
                    bppBill += billList.size();
                }
            }
            List<BppOrder> bppOrderList = bppOrderApiService.findOrderAll(ApiRequest.newInstance().filterEqual(QBppOrder.areaId, areaId));
            if (!CollectionUtils.isEmpty(bppOrderList)) {
                for (BppOrder bppOrder : bppOrderList) {
                    List<BppOrderDetail> bppOrderDetails = bppOrderApiService.findByOrderId(bppOrder.getId());
                    if (!CollectionUtils.isEmpty(bppOrderDetails)) {
                        bppOrderApiService.updateOrderFeeId(bppOrder.getId(), bppOrderDetails.get(0).getFeeId());
                        bppOrderNum += 1;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("刷新财务数据出错 : ", e);
            isSucess = false;
        }

        importTask.setBppFeeScaleNum(bppFeeScaleNum);
        importTask.setBillNum(bppBill);
        importTask.setOrderNum(bppOrderNum);
        importTaskApiService.updateNum(importTask);

        if (!isSucess) {
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FAILED, "刷新财务数据错误");
            return;
        }

        importEventBean.setPart(ImportDataPartConstants.STAFF);
        importEventBean.setStartId(0);
        eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.FLUSH_AREA_DATA_EVENT, importEventBean));

    }

    private void flushStaff(Integer startId, Integer areaId, ImportTask importTask, ImportEventBean importEventBean) {
        logger.info("开始员工, areaId = {}", areaId);
        AreaInfo areaInfo = smartAreaInfoApiService.findOne(areaId);
        if (areaInfo == null) {
            logger.error("小区信息未找到, id = " + areaId);
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FAILED, "刷新员工数据错误");
            return;
        }
        ApiResponse<SmartUserAccount> response = smartOauth2UserAccountApiService.findAccountAll(ApiRequest.newInstance().filterEqual(QSmartUserAccount.tenantId, areaInfo.getUniqueCode()).filterGreaterThan(QSmartUserAccount.id, startId), ApiRequestPage.newInstance().paging(0, PAGE_SIZE).addOrder(QSmartUserAccount.id, PageOrderType.ASC));
        int staffNum = 0;
        int lastId = 0;
        boolean isSucess = true;
        boolean isFirst = false;
        if (startId == 0) {
            isFirst = true;
        }
        if (!CollectionUtils.isEmpty(response.getPagedData())) {
            for (SmartUserAccount smartUserAccount : response.getPagedData()) {
                try {

                    List<SmartOauth2Account> smartOauth2Accounts = smartOauth2UserAccountApiService.findAll(ApiRequest.newInstance().filterEqual(QSmartOauth2Account.accountId, smartUserAccount.getId()));
                    if (CollectionUtils.isEmpty(smartOauth2Accounts)) {
                        logger.error("没有授权信息, id = " + smartUserAccount.getId());
                        lastId = smartUserAccount.getId().intValue();
                        continue;
                    }
                    if (smartOauth2Accounts.size() == 1 && smartOauth2Accounts.get(0).getClientId().equals(externalClientId)) {
                        logger.error("是外部用户, id = " + smartUserAccount.getId());
                        lastId = smartUserAccount.getId().intValue();
                        continue;
                    }
                    cn.lehome.base.api.common.business.oauth2.bean.user.UserAccount userAccount = businessUserAccountApiService.getAccountByPhone(smartUserAccount.getPhoneNumber(), smartUserAccount.getTenantId());
                    if (userAccount != null) {
                        logger.error("员工信息已经存在, id = " + smartUserAccount.getId());
                        lastId = smartUserAccount.getId().intValue();
                        continue;
                    }
                    userAccount = new cn.lehome.base.api.common.business.oauth2.bean.user.UserAccount();
                    userAccount.setSalt(smartUserAccount.getSalt());
                    userAccount.setTenantId(smartUserAccount.getTenantId());
                    userAccount.setPhone(smartUserAccount.getPhoneNumber());
                    userAccount.setUserKey(smartUserAccount.getPhoneNumber());
                    userAccount.setSecret(smartUserAccount.getSecret());
                    userAccount.setType(AccountType.EMPLOYEE_ACCOUNT);
                    userAccount = businessUserAccountApiService.createAccount(userAccount, "sqbj-smart");
                    Oauth2Account oauth2Account = businessUserAccountApiService.getOauth2Account(userAccount.getId(), "sqbj-smart");
                    for (SmartOauth2Account smartOauth2Account : smartOauth2Accounts) {
                        if (!smartOauth2Account.getClientId().equals(externalClientId)) {
                            businessUserAccountApiService.addMutiOpenId(userAccount.getId(), oauth2Account.getId(), smartOauth2Account.getUserOpenId());
                        }
                    }
                    staffNum += 1;
                    List<RoleUsers> roleUserses = smartAbacApiService.findByUniqueId(smartUserAccount.getUniqueId());
                    if (!CollectionUtils.isEmpty(roleUserses)) {
                        ArrayListMultimap<Long, String> roleMultiMap = ArrayListMultimap.create();
                        for (RoleUsers roleUsers : roleUserses) {
                            if (!roleUsers.getRegionId().contains("MANAGE_AREA")) {
                                continue;
                            }
                            String[] args = roleUsers.getRegionId().split(":");
                            Long areaid = 0L;
                            try {
                                areaid = Long.parseLong(args[1]);
                            } catch (Exception e) {
                                logger.error("转换小区ID失败, areaId = :" + args[1]);
                                continue;
                            }
                            roleMultiMap.put(areaid, roleUsers.getRoleKey());
                        }
                        for (Long areaid : roleMultiMap.keySet()) {
                            businessUserAccountApiService.addArea(userAccount.getId(), areaid, YesNoStatus.NO);
                            List<RoleMapper> roleMappers = roleTemplateApiService.findMapperAll(ApiRequest.newInstance().filterIn(QRoleMapper.roleKey, roleMultiMap.get(areaid)));
                            if (!CollectionUtils.isEmpty(roleMappers)) {
                                List<SysRole> sysRoles = sysRoleApiService.findAll(ApiRequest.newInstance().filterEqual(QSysRole.objectId, areaId).filterEqual(QSysRole.type, RoleType.PROJECT_ROLE).filterIn(QSysRole.name, roleMappers.stream().map(RoleMapper::getNewName).collect(Collectors.toList())));
                                if (!CollectionUtils.isEmpty(sysRoles)) {
                                    List<SysUsersRoles> sysUsersRolesList = Lists.newArrayList();
                                    for (SysRole sysRole : sysRoles) {
                                        SysUsersRoles sysUsersRoles = new SysUsersRoles();
                                        sysUsersRoles.setObjectId(areaid.toString());
                                        sysUsersRoles.setRoleType(RoleType.PROJECT_ROLE);
                                        sysUsersRoles.setSysRolesId(sysRole.getId());
                                        sysUsersRoles.setSysUsersId(userAccount.getId());
                                        sysUsersRolesList.add(sysUsersRoles);
                                    }
                                    sysRoleApiService.createUpdateUserRoles(userAccount.getId(), sysUsersRolesList, RoleType.PROJECT_ROLE);
                                }
                            }

                        }
                    }
                    lastId = smartUserAccount.getId().intValue();
                } catch (Exception e) {
                    logger.error("刷新员工数据出错, id = " + smartUserAccount.getId(), e);
                    isSucess = false;
                    break;
                }
            }

            if (!isFirst) {
                importTask.setStaffNum(importTask.getStaffNum() + staffNum);
            } else {
                importTask.setStaffNum(staffNum);
            }

            importTaskApiService.updateNum(importTask);
            if (!isSucess) {
                importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FAILED, "刷新员工错误");
                return;
            }


            if (response.getCount() == 0 || response.getCount() < PAGE_SIZE) {
                importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FINISHED, "");
            } else {
                importEventBean.setPart(ImportDataPartConstants.STAFF);
                importEventBean.setStartId(lastId);
                eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.FLUSH_AREA_DATA_EVENT, importEventBean));
            }
        } else {
            importTaskApiService.updateStatus(importTask.getId(), ImportTaskStatus.FINISHED, "");
        }
    }

    @Override
    public String getConsumerId() {
        return "area_flush";
    }



}
