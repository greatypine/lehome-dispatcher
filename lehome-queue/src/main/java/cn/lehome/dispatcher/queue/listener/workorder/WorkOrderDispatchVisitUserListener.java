package cn.lehome.dispatcher.queue.listener.workorder;

import cn.lehome.base.api.common.business.oauth2.bean.sys.QSysRole;
import cn.lehome.base.api.common.business.oauth2.bean.sys.SysRole;
import cn.lehome.base.api.common.business.oauth2.bean.sys.SysRolesAuth;
import cn.lehome.base.api.common.business.oauth2.bean.user.*;
import cn.lehome.base.api.common.business.oauth2.service.sys.SysRoleApiService;
import cn.lehome.base.api.common.business.oauth2.service.user.UserAccountIndexApiService;
import cn.lehome.base.api.common.operation.bean.resource.QResources;
import cn.lehome.base.api.common.operation.bean.resource.Resources;
import cn.lehome.base.api.common.operation.service.resource.ResourceApiService;
import cn.lehome.base.api.workorder.bean.customercenter.BusinessAcceptOrder;
import cn.lehome.base.api.workorder.bean.customercenter.BusinessOrderUserStatistics;
import cn.lehome.base.api.workorder.bean.customercenter.QBusinessOrderUserStatistics;
import cn.lehome.base.api.workorder.bean.settings.BusinessTypeUserSetting;
import cn.lehome.base.api.workorder.service.customercenter.BusinessAcceptOrderApiService;
import cn.lehome.base.api.workorder.service.customercenter.BusinessOrderUserStatisticsApiService;
import cn.lehome.base.api.workorder.service.settings.BusinessTypeUserSettingApiService;
import cn.lehome.bean.workorder.enums.YesOrNo;
import cn.lehome.bean.workorder.enums.customercenter.BusinessAcceptStatus;
import cn.lehome.common.bean.business.oauth2.enums.sys.RoleType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.util.AuthPermissionValueUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2019/5/31.
 */
public class WorkOrderDispatchVisitUserListener extends AbstractJobListener {

    @Autowired
    private BusinessAcceptOrderApiService businessAcceptOrderApiService;

    @Autowired
    private BusinessTypeUserSettingApiService businessTypeUserSettingApiService;

    @Autowired
    private UserAccountIndexApiService businessUserAccountIndexApiService;

    @Autowired
    private SysRoleApiService sysRoleApiService;

    @Autowired
    private ResourceApiService resourceApiService;

    @Autowired
    private BusinessOrderUserStatisticsApiService businessOrderUserStatisticsApiService;

    @Value("${workorder.return.visit.resource.key}")
    private String returnVisitResourceKey;


    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        BusinessAcceptOrder businessAcceptOrder = businessAcceptOrderApiService.get(longEventMessage.getData().intValue());
        if (businessAcceptOrder == null) {
            logger.error("工单信息未找到, id = {}", longEventMessage.getData());
            return;
        }
        List<BusinessTypeUserSetting> businessTypeUserSettingList = businessTypeUserSettingApiService.findByAreaIdAndTopTypeIdAndStatus(businessAcceptOrder.getAreaId().longValue(), businessAcceptOrder.getTopLevelTypeId().longValue(), BusinessAcceptStatus.ReturnVisited);
        Map<String, UserAccountIndex> userOpenIds = Maps.newHashMap();
        if (!CollectionUtils.isEmpty(businessTypeUserSettingList)) {
            if (businessTypeUserSettingList.get(0).getIsDepartment().equals(YesOrNo.Yes)) {
                // Todo 按照部门获取用户信息
            } else {
                for (BusinessTypeUserSetting businessTypeUserSetting : businessTypeUserSettingList) {
                    List<UserAccountIndex> userAccountIndices = businessUserAccountIndexApiService.findAccountAll(ApiRequest.newInstance().cascadeChild("oauth2_account", ApiRequest.newInstance().filterIn(QOauth2AccountIndex.mutiOpenId, Lists.newArrayList(businessTypeUserSetting.getObjectId()))));
                    if (!CollectionUtils.isEmpty(userAccountIndices)) {
                        userOpenIds.put(businessTypeUserSetting.getObjectId(), userAccountIndices.get(0));
                    }
                }

            }
        }

        if (CollectionUtils.isEmpty(userOpenIds)) {
            List<SysRole> sysRoles = sysRoleApiService.findAll(ApiRequest.newInstance().filterEqual(QSysRole.objectId, businessAcceptOrder.getAreaId()).filterEqual(QSysRole.type, RoleType.PROJECT_ROLE));
            if (CollectionUtils.isEmpty(sysRoles)) {
                logger.error("未找到角色信息");
                return;
            }
            Map<Long, List<SysRolesAuth>> listMap = sysRoleApiService.findByRoleIds(sysRoles.stream().map(SysRole::getId).collect(Collectors.toList()));
            List<Resources> resourcesList = resourceApiService.findAll(ApiRequest.newInstance().filterEqual(QResources.key, returnVisitResourceKey));
            if (CollectionUtils.isEmpty(resourcesList)) {
                logger.error("未找到资源信息");
                return;
            }
            Set<Long> roleIdSet = Sets.newHashSet();
            for (Long sysRoleId : listMap.keySet()) {
                List<SysRolesAuth> sysRolesAuthList = listMap.get(sysRoleId);
                for (SysRolesAuth sysRolesAuth : sysRolesAuthList) {
                    if (AuthPermissionValueUtils.checkAuth(sysRolesAuth.getPermissionValue(), Lists.newArrayList(resourcesList.get(0).getAuthValue()))) {
                        roleIdSet.add(sysRoleId);
                        break;
                    }
                }
            }
            List<UserAccountIndex> userAccountIndices = businessUserAccountIndexApiService.findAccountAll(ApiRequest.newInstance().cascadeChild("user_area", ApiRequest.newInstance().filterEqual(QUserAccountAreaIndex.areaId, businessAcceptOrder.getAreaId())).cascadeChild("user_roles", ApiRequest.newInstance().filterIn(QUserRolesIndex.sysRolesId, roleIdSet)));
            if (!CollectionUtils.isEmpty(userAccountIndices)) {
                Map<String, UserAccountIndex> userAccountIndexMap = userAccountIndices.stream().collect(Collectors.toMap(UserAccountIndex::getId, userAccountIndex -> userAccountIndex));
                List<Oauth2AccountIndex> oauth2AccountIndexList = businessUserAccountIndexApiService.findAll(ApiRequest.newInstance().filterIn(QOauth2AccountIndex.accountId, userAccountIndices.stream().map(UserAccountIndex::getId).collect(Collectors.toList())).filterEqual(QOauth2AccountIndex.clientId, "sqbj-smart"));
                if (!CollectionUtils.isEmpty(oauth2AccountIndexList)) {
                   for (Oauth2AccountIndex oauth2AccountIndex : oauth2AccountIndexList) {
                       UserAccountIndex userAccountIndex = userAccountIndexMap.get(oauth2AccountIndex.getAccountId());
                       if (userAccountIndex != null) {
                           userOpenIds.put(oauth2AccountIndex.getUserOpenId(), userAccountIndex);
                       }
                   }
                }
            }
        }

        if (CollectionUtils.isEmpty(userOpenIds)) {
            logger.error("未找到符合的用户信息, id = {}", businessAcceptOrder.getId());
            return;
        }

        List<BusinessOrderUserStatistics> businessOrderUserStatisticses = businessOrderUserStatisticsApiService.findAll(ApiRequest.newInstance().filterEqual(QBusinessOrderUserStatistics.areaId, businessAcceptOrder.getAreaId()).filterIn(QBusinessOrderUserStatistics.userOpenId, userOpenIds.keySet()));
        Map<String, BusinessOrderUserStatistics> businessOrderUserStatisticsMap = Maps.newHashMap();
        if (!CollectionUtils.isEmpty(businessOrderUserStatisticses)) {
            businessOrderUserStatisticses.forEach(businessOrderUserStatistics -> businessOrderUserStatisticsMap.put(businessOrderUserStatistics.getUserOpenId(), businessOrderUserStatistics));
        }
        for (String userOpenId : userOpenIds.keySet()) {
            BusinessOrderUserStatistics businessOrderUserStatistics = businessOrderUserStatisticsMap.get(userOpenId);
            if (businessOrderUserStatistics == null) {
                businessOrderUserStatistics = new BusinessOrderUserStatistics();
                businessOrderUserStatistics.setUserOpenId(userOpenId);
                businessOrderUserStatistics.setAreaId(businessAcceptOrder.getAreaId());
                businessOrderUserStatistics.setReturnVisitNum(0L);
                businessOrderUserStatistics.setFinishedAcceptingNum(0L);
                businessOrderUserStatisticsMap.put(businessOrderUserStatistics.getUserOpenId(), businessOrderUserStatistics);
            }
        }
        List<BusinessOrderUserStatistics> sorted = Lists.newArrayList(businessOrderUserStatisticsMap.values());
        Collections.sort(sorted, (o1, o2) -> {
            if (Long.compare(o1.getReturnVisitNum(), o2.getReturnVisitNum()) == 0) {
                return Long.compare(o1.getFinishedAcceptingNum(), o2.getFinishedAcceptingNum());
            } else {
                return Long.compare(o1.getReturnVisitNum(), o2.getReturnVisitNum());
            }
        });
        businessAcceptOrderApiService.dispatchReturnVisit(businessAcceptOrder.getId(), sorted.get(0).getUserOpenId());
    }

    @Override
    public String getConsumerId() {
        return "work_order_dispatch_return_visit_user";
    }

}
