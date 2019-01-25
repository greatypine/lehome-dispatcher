package cn.lehome.dispatcher.utils.community;

import cn.lehome.base.api.bigdata.bean.SmartCommunityReport;
import cn.lehome.base.api.bigdata.service.smart.SmartCommunityReportApiService;
import cn.lehome.base.api.thirdparty.bean.smart.SmartOpenDoorRequest;
import cn.lehome.base.api.thirdparty.bean.smart.SmartOpenDoorResponse;
import cn.lehome.base.api.thirdparty.service.smart.SmartHttpRequestApiService;
import cn.lehome.base.api.tool.bean.community.CommunityExt;
import cn.lehome.base.api.tool.bean.community.QCommunityExt;
import cn.lehome.base.api.tool.service.community.CommunityApiService;
import cn.lehome.base.api.tool.util.DateUtil;
import cn.lehome.bean.bigdata.enums.DataIncreEnum;
import cn.lehome.bean.tool.entity.enums.community.EditionType;
import cn.lehome.framework.base.api.core.compoment.request.ApiPageRequestHelper;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;

@Service
public class SmartDataServiceImpl implements SmartDataService {

    @Autowired
    private SmartHttpRequestApiService smartHttpRequestApiService;

    @Autowired
    private SmartCommunityReportApiService smartCommunityReportApiService;

    @Autowired
    private CommunityApiService communityApiService;

    private static DateFormat dateFormat_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static DateFormat dateFormatNoTime = new SimpleDateFormat("yyyy-MM-dd");


    @Override
    public void execOpenDoorData(String[] args) throws ParseException {
        if(args.length < 3){
            System.out.println("参数不够");
            return;
        }
        System.out.println("智社区开门数据重跑开始...");
        List<CommunityExt> communityExts = Lists.newArrayList();
        Set<Long> communityIds = Sets.newHashSet();
        if (args.length == 3) {
            int pageIndex = 0;
            int pageSize = 10;
            ApiRequest apiRequest = ApiRequest.newInstance();
            apiRequest.filterEqual(QCommunityExt.editionType, EditionType.pro);
            apiRequest.filterEqual(QCommunityExt.enableStatus, EnableDisableStatus.ENABLE);

            communityExts = ApiPageRequestHelper.request(apiRequest, ApiRequestPage.newInstance().paging(pageIndex, pageSize),
                    communityApiService::findAllExt);
        } else {
            communityExts.add(communityApiService.getExt(Long.valueOf(args[3])));
        }

        if (CollectionUtils.isEmpty(communityExts)) {
            System.out.println("无重跑的小区数据");
            return;
        }
        Date startDate = dateFormatNoTime.parse(args[1]);
        startDate = DateUtils.setHours(startDate, 0);
        startDate = DateUtils.setMinutes(startDate, 0);
        startDate = DateUtils.setMilliseconds(startDate, 0);
        final Date finalStartDate = DateUtils.setSeconds(startDate, 0);

        Date endDate = dateFormatNoTime.parse(args[2]);
        endDate = DateUtils.setHours(endDate, 0);
        endDate = DateUtils.setMinutes(endDate, 0);
        endDate = DateUtils.setMilliseconds(endDate, 0);
        final Date finalEndDate = DateUtils.setSeconds(endDate, 0);
        communityExts.parallelStream().forEach(communityExt -> {
            if (!communityIds.contains(communityExt.getId()) && communityExt.getPropertyCommunityId() != 0L) {
                exec(communityExt, finalStartDate, finalEndDate);

                communityIds.add(communityExt.getId());
            }
        });
        System.out.println("智社区开门数据重跑完成！");
    }

    private void exec(CommunityExt communityExt, Date startDate, Date endDate) {
        while (DateUtil.getDateBetween(startDate, endDate) >= 0) {
            SmartOpenDoorRequest request = new SmartOpenDoorRequest();

            request.setStartDate(dateFormat_time.format(startDate));
            Date endTime = DateUtil.addDays(startDate, 1);
            Date queryEndTime = DateUtils.addSeconds(endTime, -1);
            request.setEndDate(dateFormat_time.format(queryEndTime));

            request.setCommunityId(communityExt.getPropertyCommunityId());
            System.out.println("获取这社区小区开门数据请求参数 : " + JSON.toJSONString(request));
            SmartOpenDoorResponse smartOpenDoorResponse = null;
            try {
                smartOpenDoorResponse = smartHttpRequestApiService.openDoorData(request);
            } catch (Exception e) {
                System.out.println("调用智社区获取数据报错, request =" + JSON.toJSONString(request));
                smartOpenDoorResponse = null;
            }

            System.out.println("获取这社区小区开门数据请求结果 : " + JSON.toJSONString(smartOpenDoorResponse));
            if (smartOpenDoorResponse != null) {
                SmartCommunityReport smartCommunityReport = new SmartCommunityReport();
                smartCommunityReport.setCommunityId(communityExt.getId());
                smartCommunityReport.setPropertyCommunityId(communityExt.getPropertyCommunityId());
                smartCommunityReport.setOpenDoorNum(smartOpenDoorResponse.getCount());
                smartCommunityReport.setOpenDoorUserNum(smartOpenDoorResponse.getUserCount());
                smartCommunityReport.setStatDateTime(startDate);
                smartCommunityReport.setBeginDateTime(startDate);
                smartCommunityReport.setEndDateTime(endTime);
                smartCommunityReport.setTotalOrIncre(DataIncreEnum.DAY);

                smartCommunityReportApiService.saveOrUpdate(smartCommunityReport);

            }
            if (getFirstDayOfMonth(startDate).equals(startDate)) {
                // 月初跑上一整月的数据
                Date lastMonthFirstDay = getFirstDayOfMonth(DateUtil.addDays(startDate, -1));
                request.setStartDate(dateFormat_time.format(lastMonthFirstDay));
                Date queryEndDate = DateUtils.addSeconds(startDate, -1);
                request.setEndDate(dateFormat_time.format(queryEndDate));
                try {
                    smartOpenDoorResponse = smartHttpRequestApiService.openDoorData(request);
                } catch (Exception e) {
                    System.out.println("调用智社区获取数据报错, request=" + JSON.toJSONString(request));
                    smartOpenDoorResponse = null;
                }
                if (smartOpenDoorResponse != null) {
                    SmartCommunityReport smartCommunityReport = new SmartCommunityReport();
                    smartCommunityReport.setCommunityId(communityExt.getId());
                    smartCommunityReport.setPropertyCommunityId(communityExt.getPropertyCommunityId());
                    smartCommunityReport.setOpenDoorNum(smartOpenDoorResponse.getCount());
                    smartCommunityReport.setOpenDoorUserNum(smartOpenDoorResponse.getUserCount());
                    smartCommunityReport.setStatDateTime(lastMonthFirstDay);
                    smartCommunityReport.setBeginDateTime(lastMonthFirstDay);
                    smartCommunityReport.setEndDateTime(startDate);
                    smartCommunityReport.setTotalOrIncre(DataIncreEnum.DAY);

                    smartCommunityReportApiService.saveOrUpdate(smartCommunityReport);
                }
            } else {
                Date thisMonthFirstDay = getFirstDayOfMonth(startDate);
                request.setStartDate(dateFormat_time.format(thisMonthFirstDay));
                try {
                    smartOpenDoorResponse = smartHttpRequestApiService.openDoorData(request);
                } catch (Exception e) {
                    System.out.println("调用智社区获取数据报错, request =" + JSON.toJSONString(request));
                    smartOpenDoorResponse = null;
                }
                if (smartOpenDoorResponse != null) {
                    SmartCommunityReport smartCommunityReport = new SmartCommunityReport();
                    smartCommunityReport.setCommunityId(communityExt.getId());
                    smartCommunityReport.setPropertyCommunityId(communityExt.getPropertyCommunityId());
                    smartCommunityReport.setOpenDoorNum(smartOpenDoorResponse.getCount());
                    smartCommunityReport.setOpenDoorUserNum(smartOpenDoorResponse.getUserCount());
                    smartCommunityReport.setStatDateTime(startDate);
                    smartCommunityReport.setBeginDateTime(thisMonthFirstDay);
                    smartCommunityReport.setEndDateTime(startDate);
                    smartCommunityReport.setTotalOrIncre(DataIncreEnum.DAY);

                    smartCommunityReportApiService.saveOrUpdate(smartCommunityReport);
                }
            }
            startDate = DateUtil.addDays(startDate, 1);
        }

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
}
