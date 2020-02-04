package cn.lehome.dispatcher.quartz.service.invoke;

//import cn.lehome.base.api.bigdata.service.smart.SmartCommunityReportApiService;
import cn.lehome.base.api.business.ec.service.partner.PartnerCommunityApiService;
import cn.lehome.base.api.common.bean.community.CommunityExt;
import cn.lehome.base.api.common.bean.community.QCommunityExt;
import cn.lehome.base.api.common.service.community.CommunityApiService;
import cn.lehome.base.api.common.service.community.CommunityCacheApiService;
import cn.lehome.bean.common.enums.community.EditionType;
import cn.lehome.dispatcher.quartz.service.AbstractInvokeServiceImpl;
import cn.lehome.framework.base.api.core.compoment.request.ApiPageRequestHelper;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 智社区开门数据定时任务处理类
 * 2018-05-10
 */
@Service("smartOpenDoorDataService")
public class SmartOpenDoorDataServiceImpl extends AbstractInvokeServiceImpl {

    //@Autowired
    //private PartnerCommunityApiService partnerCommunityApiService;

//    @Autowired
//    private SmartHttpRequestApiService smartHttpRequestApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private CommunityApiService communityApiService;

//    @Autowired
//    private SmartCommunityReportApiService smartCommunityReportApiService;

    private static DateFormat dateFormat_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * 今天跑前一天的数据,月初则把上一整月的数据跑出来
     */
    @Override
    public void doInvoke(Map<String, String> params) {
        logger.info("智社区开门数据同步start");

        int pageIndex = 0;
        int pageSize = 10;
        ApiRequest apiRequest = ApiRequest.newInstance();
        apiRequest.filterEqual(QCommunityExt.editionType, EditionType.pro);
        apiRequest.filterEqual(QCommunityExt.enableStatus, EnableDisableStatus.ENABLE);
        Set<Long> communityIds = Sets.newHashSet();

        List<CommunityExt> communityExts = ApiPageRequestHelper.request(apiRequest, ApiRequestPage.newInstance().paging(pageIndex, pageSize),
                communityApiService::findAllExt);

        if (!CollectionUtils.isEmpty(communityExts)) {
            communityExts.parallelStream().forEach(communityExt -> {
//                SmartOpenDoorRequest request = new SmartOpenDoorRequest();
//
//                Date now = new Date();
//
//                Date endTime = DateUtils.setHours(now, 0);
//                endTime = DateUtils.setMinutes(endTime, 0);
//                endTime = DateUtils.setSeconds(endTime, 0);
//                endTime = DateUtils.setMilliseconds(endTime, 0);
//                Date startTime = DateUtils.addDays(endTime, -1);
//
//                request.setStartDate(dateFormat_time.format(startTime));
//                // 结束时间是23:59:59
//                Date queryEndTime = DateUtils.addSeconds(endTime, -1);
//                request.setEndDate(dateFormat_time.format(queryEndTime));
//
//                if (communityExt != null && communityExt.getPropertyCommunityId() != 0L) {
//                    if (!communityIds.contains(communityExt.getId())) {
//                        request.setCommunityId(communityExt.getPropertyCommunityId());
//                        logger.error("获取这社区小区开门数据请求参数 : " + JSON.toJSONString(request));
//                        SmartOpenDoorResponse smartOpenDoorResponse = null;
//                        try {
//                            smartOpenDoorResponse = smartHttpRequestApiService.openDoorData(request);
//                        } catch (Exception e) {
//                            logger.error("调用智社区获取数据报错, request = {}", JSON.toJSONString(request));
//                            smartOpenDoorResponse = null;
//                        }
//
//                        logger.error("获取这社区小区开门数据请求结果 : " + JSON.toJSONString(smartOpenDoorResponse));
//                        if (smartOpenDoorResponse != null) {
//                            SmartCommunityReport smartCommunityReport = new SmartCommunityReport();
//                            smartCommunityReport.setCommunityId(communityExt.getId());
//                            smartCommunityReport.setPropertyCommunityId(communityExt.getPropertyCommunityId());
//                            smartCommunityReport.setOpenDoorNum(smartOpenDoorResponse.getCount());
//                            smartCommunityReport.setOpenDoorUserNum(smartOpenDoorResponse.getUserCount());
//                            smartCommunityReport.setStatDateTime(DateUtil.getDayBegin(startTime));
//                            smartCommunityReport.setBeginDateTime(DateUtil.getDayBegin(startTime));
//                            smartCommunityReport.setEndDateTime(DateUtil.getDayBegin(now));
//                            smartCommunityReport.setTotalOrIncre(DataIncreEnum.DAY);
//
//                            smartCommunityReportApiService.save(smartCommunityReport);
//
//                        }
//                        if (getFirstDayOfMonth(endTime).equals(endTime)) {
//                            // 月初跑上一整月的数据
//                            Date lastMonthFirstDay = getFirstDayOfMonth(startTime);
//                            request.setStartDate(dateFormat_time.format(lastMonthFirstDay));
//                            try {
//                                smartOpenDoorResponse = smartHttpRequestApiService.openDoorData(request);
//                            } catch (Exception e) {
//                                logger.error("调用智社区获取数据报错, request = {}", JSON.toJSONString(request));
//                                smartOpenDoorResponse = null;
//                            }
//                            if (smartOpenDoorResponse != null) {
//                                SmartCommunityReport smartCommunityReport = new SmartCommunityReport();
//                                smartCommunityReport.setCommunityId(communityExt.getId());
//                                smartCommunityReport.setPropertyCommunityId(communityExt.getPropertyCommunityId());
//                                smartCommunityReport.setOpenDoorNum(smartOpenDoorResponse.getCount());
//                                smartCommunityReport.setOpenDoorUserNum(smartOpenDoorResponse.getUserCount());
//                                smartCommunityReport.setStatDateTime(lastMonthFirstDay);
//                                smartCommunityReport.setBeginDateTime(lastMonthFirstDay);
//                                smartCommunityReport.setEndDateTime(DateUtil.getDayBegin(now));
//                                smartCommunityReport.setTotalOrIncre(DataIncreEnum.DAY);
//
//                                smartCommunityReportApiService.save(smartCommunityReport);
//                            }
//                        } else {
//                            Date thisMonthFirstDay = getFirstDayOfMonth(startTime);
//                            request.setStartDate(dateFormat_time.format(thisMonthFirstDay));
//                            try {
//                                smartOpenDoorResponse = smartHttpRequestApiService.openDoorData(request);
//                            } catch (Exception e) {
//                                logger.error("调用智社区获取数据报错, request = {}", JSON.toJSONString(request));
//                                smartOpenDoorResponse = null;
//                            }
//                            if (smartOpenDoorResponse != null) {
//                                SmartCommunityReport smartCommunityReport = new SmartCommunityReport();
//                                smartCommunityReport.setCommunityId(communityExt.getId());
//                                smartCommunityReport.setPropertyCommunityId(communityExt.getPropertyCommunityId());
//                                smartCommunityReport.setOpenDoorNum(smartOpenDoorResponse.getCount());
//                                smartCommunityReport.setOpenDoorUserNum(smartOpenDoorResponse.getUserCount());
//                                smartCommunityReport.setStatDateTime(endTime);
//                                smartCommunityReport.setBeginDateTime(thisMonthFirstDay);
//                                smartCommunityReport.setEndDateTime(endTime);
//                                smartCommunityReport.setTotalOrIncre(DataIncreEnum.DAY);
//
//                                smartCommunityReportApiService.save(smartCommunityReport);
//                            }
//                        }
//                        communityIds.add(communityExt.getId());
//                    }
//                }
            });
        }


        logger.info("智社区开门数据同步end");
    }

    /**
     * 获取指定日期的月的第一天
     * @param date 指定日期
     * @return 指定日期的月的第一天
     */
    public Date getFirstDayOfMonth(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        return cal.getTime();
    }

    public static void main (String[] args) {
        Date now = new Date();

        Date endTime = DateUtils.setHours(now, 0);
        endTime = DateUtils.setMinutes(endTime, 0);
        endTime = DateUtils.setSeconds(endTime, 0);
        endTime = DateUtils.addSeconds(endTime, -1);
        System.out.println(dateFormat_time.format(endTime));
    }
}
