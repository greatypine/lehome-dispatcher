package cn.lehome.dispatcher.quartz.service.invoke.device;

import cn.lehome.base.api.acs.bean.region.Region;
import cn.lehome.base.api.acs.service.region.RegionApiService;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.base.api.iot.common.bean.entrance.EntranceDevice;
import cn.lehome.base.api.iot.common.bean.entrance.QEntranceDevice;
import cn.lehome.base.api.iot.common.service.entrance.EntranceDeviceApiService;
import cn.lehome.base.api.workorder.bean.event.Event;
import cn.lehome.base.pro.api.bean.address.AddressBaseInfo;
import cn.lehome.base.pro.api.bean.area.AreaInfo;
import cn.lehome.base.pro.api.bean.house.AddressBean;
import cn.lehome.base.pro.api.bean.regions.ControlRegionsPositionRelationship;
import cn.lehome.base.pro.api.bean.regions.QControlRegionsPositionRelationship;
import cn.lehome.base.pro.api.service.address.AddressBaseApiService;
import cn.lehome.base.pro.api.service.area.AreaInfoApiService;
import cn.lehome.base.pro.api.service.regions.ControlRegionsApiService;
import cn.lehome.base.pro.api.service.regions.ControlRegionsPositionRelationshipApiService;
import cn.lehome.bean.pro.enums.address.ExtendType;
import cn.lehome.bean.workorder.enums.event.EventType;
import cn.lehome.bean.workorder.enums.event.PositionType;
import cn.lehome.bean.workorder.enums.event.TargetType;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.iot.bean.common.enums.entrance.EntranceDeviceType;
import cn.lehome.iot.bean.common.enums.gateway.OnlineStatus;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by wuzhao on 2019/10/11.
 */
@Service("entranceStatusJobService")
public class EntranceStatusJobServiceImpl extends AbstractInvokeServiceImpl {

    @Autowired
    private EntranceDeviceApiService entranceDeviceApiService;

    @Autowired
    private RegionApiService regionApiService;

    @Autowired
    private ControlRegionsApiService controlRegionsApiService;

    @Autowired
    private ControlRegionsPositionRelationshipApiService controlRegionsPositionRelationshipApiService;

    @Autowired
    private AddressBaseApiService addressBaseApiService;

    @Autowired
    private AreaInfoApiService proAreaInfoApiService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private static int ONLINE_INTERVAL = -2;

    private static long offlineTime = 2 * 60 * 60 * 1000l;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Override
    public void doInvoke(Map<String, String> params) {
        Date date = new Date();
        date = DateUtils.addMinutes(date, ONLINE_INTERVAL);
        ApiRequest apiRequest = ApiRequest.newInstance().filterLessThan(QEntranceDevice.lastOnlineTime, date);
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance().paging(0, 50);
        while (true) {
            ApiResponse<EntranceDevice> apiResponse = entranceDeviceApiService.findAll(apiRequest, apiRequestPage);

            if (apiResponse == null || apiResponse.getCount() == 0) {
                break;
            }

            List<EntranceDevice> entranceDeviceList = Lists.newArrayList(apiResponse.getPagedData());

            List<EntranceDevice> updateList = Lists.newArrayList();

            for (EntranceDevice entranceDevice : entranceDeviceList) {
                if (entranceDevice.getOnlineStatus().equals(OnlineStatus.ONLINE)) {
                    if (entranceDevice.getDeviceType().equals(EntranceDeviceType.NFC_2G)) {
                        String key = String.format("%s_%s", "base", entranceDevice.getDeviceUuid());
                        if (!stringRedisTemplate.hasKey(key)) {
                            entranceDevice.setOnlineStatus(OnlineStatus.OFFLINE);
                            updateList.add(entranceDevice);
                        }
                    } else {
                        updateList.add(entranceDevice);
                    }
                } else {
                    if (System.currentTimeMillis() - entranceDevice.getUpdatedTime().getTime() >= offlineTime) {
                        Region region = regionApiService.findByDeviceUuid(entranceDevice.getDeviceUuid());
                        if (region != null && !region.getDisabled())  {
                            List<ControlRegionsPositionRelationship> regionsPositionRelationships = controlRegionsPositionRelationshipApiService.findAll(ApiRequest.newInstance().filterEqual(QControlRegionsPositionRelationship.controlRegionId, region.getTraceId()));
                            if (!CollectionUtils.isEmpty(regionsPositionRelationships)) {
                                ControlRegionsPositionRelationship relationship = regionsPositionRelationships.get(0);
                                AreaInfo areaInfo = proAreaInfoApiService.findOne(relationship.getCommunityId().intValue());
                                AddressBaseInfo addressBaseInfo = null;
                                PositionType positionType = PositionType.UNIT;
                                if (relationship.getUnitId() != null && relationship.getUnitId() != 0) {
                                    addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.UNIT, relationship.getUnitId());
                                } else {
                                    if (relationship.getFloorId() != null && relationship.getFloorId() != 0) {
                                        addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.BUILDING, relationship.getFloorId());
                                        positionType = PositionType.BUILDING;
                                    } else {
                                        if (relationship.getProjectId() != null && relationship.getProjectId() != 0) {
                                            addressBaseInfo = addressBaseApiService.findByExtendId(ExtendType.PROJECT, relationship.getProjectId());
                                            positionType = PositionType.PROJECT;
                                        }
                                    }
                                }
                                if (areaInfo != null && addressBaseInfo != null) {
                                    AddressBean addressBean = JSON.parseObject(addressBaseInfo.getAddress(), AddressBean.class);
                                    String positionStr = areaInfo.getAreaName();
                                    if (positionType == PositionType.UNIT) {
                                        positionStr = positionStr + String.format("%s-%s-%s", addressBean.getProjectName(), addressBean.getBuildingNumber(), addressBean.getUnitNumber());
                                    } else if (positionType == PositionType.BUILDING) {
                                        positionStr = positionStr + String.format("%s-%s", addressBean.getProjectName(), addressBean.getBuildingNumber());
                                    } else if (positionType == PositionType.PROJECT) {
                                        positionStr = positionStr + addressBean.getProjectName();
                                    }
                                    String content = String.format("位于%s的门禁设备(%s)长期掉线", positionStr, entranceDevice.getDeviceUuid());
                                    Event event = new Event();
                                    event.setEventTime(new Date());
                                    event.setContent(content);
                                    event.setTargetType(TargetType.ENTRANCE_DEVICE);
                                    event.setTargetId(entranceDevice.getId().toString());
                                    event.setAreaId(areaInfo.getId());
                                    event.setPositionId(addressBaseInfo.getExtendId());
                                    event.setPositionType(positionType);
                                    event.setTenantId(areaInfo.getUniqueCode());
                                    event.setType(EventType.ALARM);
                                    logger.error("发送设备长期掉线告警, deviceUUid = {}", entranceDevice.getDeviceUuid());
                                    eventBusComponent.sendEventMessage(new SimpleEventMessage<>(EventConstants.EVENT_MESSAGE_EVENT, event));
                                }
                            }
                        }
                    }
                }

            }
            
            if (!CollectionUtils.isEmpty(updateList)) {
                entranceDeviceApiService.batchSave(updateList);
            }


            if (apiResponse.getCount() < apiRequestPage.getPageSize()) {
                break;
            }

            apiRequestPage.pagingNext();
        }
    }
}
