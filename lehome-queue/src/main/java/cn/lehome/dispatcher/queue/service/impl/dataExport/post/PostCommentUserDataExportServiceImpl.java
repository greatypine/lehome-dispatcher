package cn.lehome.dispatcher.queue.service.impl.dataExport.post;

import cn.lehome.base.api.business.content.bean.post.PostInfoIndex;
import cn.lehome.base.api.business.content.bean.post.QPostInfo;
import cn.lehome.base.api.business.content.bean.post.QPostInfoIndex;
import cn.lehome.base.api.business.content.bean.robot.Robot;
import cn.lehome.base.api.business.content.service.post.PostInfoIndexApiService;
import cn.lehome.base.api.business.content.service.robot.RobotApiService;
import cn.lehome.base.api.common.bean.community.Community;
import cn.lehome.base.api.common.bean.community.CommunityExt;
import cn.lehome.base.api.common.bean.dataexport.DataExportRecord;
import cn.lehome.base.api.common.bean.region.RegionInfo;
import cn.lehome.base.api.common.service.community.CommunityApiService;
import cn.lehome.base.api.common.service.community.CommunityCacheApiService;
import cn.lehome.base.api.common.service.region.RegionInfoApiService;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.business.content.enums.post.DataStatus;
import cn.lehome.bean.business.content.enums.post.UserType;
import cn.lehome.bean.common.enums.region.RegionType;
import cn.lehome.dispatcher.queue.service.impl.dataExport.AbstractDataExportServiceImpl;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by zuoguodong on 2019/9/29
 */
@Service("postCommentUserDataExportService")
public class PostCommentUserDataExportServiceImpl extends AbstractDataExportServiceImpl {

    @Autowired
    private PostInfoIndexApiService postInfoIndexApiService;

    @Autowired
    private CommunityApiService communityApiService;

    @Autowired
    private RegionInfoApiService regionInfoApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private RobotApiService robotApiService;

    @Override
    public String[] getTitle() {
        return new String[]{
                "ID",
                "发贴用户",
                "手机号",
                "小区所在地",
                "小区名称",
                "时间",
                "内容",
                "标签",
                "点赞",
                "评论",
                "PV"
        };
    }

    @Override
    public String getFileName() {
        return "发贴用户列表导出";
    }

    @Override
    public DataExportRecord exportData(DataExportRecord dataExportRecord, Long pageIndex) {
        ApiRequest apiRequest = ApiRequest.newInstance().filterEqual(QPostInfoIndex.activityId, dataExportRecord.getQueryStr()).filterLike(QPostInfo.dataStatus, DataStatus.NORMAL);
        ApiRequestPage requestPage = ApiRequestPage.newInstance().paging(pageIndex.intValue(), 10).addOrder(QPostInfoIndex.topStatus, PageOrderType.DESC).addOrder(QPostInfoIndex.createTime, PageOrderType.DESC);
        ApiResponse<PostInfoIndex> apiResponse = postInfoIndexApiService.findAll(apiRequest, requestPage);
        int count = apiResponse.getCount();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<List<String>> rowList = new ArrayList<>();
        if(count > 0){
            Set<Long> communityIds = apiResponse.getPagedData().stream().map(PostInfoIndex::getCommunityId).map(Long::valueOf).collect(Collectors.toSet());
            Map<Long, Community> communityMap = communityCacheApiService.findAllCommunity(communityIds);
            Set<Long> userIdList = Sets.newHashSet();
            Set<Long> robotIdList = Sets.newHashSet();
            apiResponse.getPagedData().forEach(postInfoRes -> {
                if (postInfoRes.getUserType().equals(UserType.APP)) {
                    userIdList.add(postInfoRes.getUserId());
                } else if (postInfoRes.getUserType().equals(UserType.ROBOT)) {
                    robotIdList.add(postInfoRes.getUserId());
                }
            });
            Map<Long, UserInfoIndex> userInfoIndexMap = Maps.newHashMap();
            if (!CollectionUtils.isEmpty(userIdList)) {
                userInfoIndexMap = userInfoIndexApiService.findAll(Lists.newArrayList(userIdList));
            }
            Map<Long, Robot> robotMap = Maps.newHashMap();
            if (!CollectionUtils.isEmpty(robotIdList)) {
                robotMap = robotApiService.findAll(robotIdList);
            }

            for(PostInfoIndex postInfoRes : apiResponse.getPagedData()){
                List<String> row = new ArrayList<>();
                row.add(postInfoRes.getPostId());

                String userName;
                if (postInfoRes.getUserType().equals(UserType.APP)) {
                    UserInfoIndex userInfoIndex = userInfoIndexMap.get(postInfoRes.getUserId());
                    if (userInfoIndex == null) {
                        logger.error("用户信息未找到, id = {}", postInfoRes.getUserId());
                    }
                    userName = userInfoIndex.getNickName();
                } else {
                    Robot robot = robotMap.get(postInfoRes.getUserId());
                    if (robot == null) {
                        logger.error("用户信息未找到, id = {}", postInfoRes.getUserId());
                    }
                    userName = robot.getNickName();
                }
                row.add(userName);
                row.add(postInfoRes.getMblNo());
                String communityName = "";
                String address = "";
                Community community = communityMap.get(Long.valueOf(postInfoRes.getCommunityId()));
                if (community == null) {
                    logger.error("小区信息未找到, communityId = {}", postInfoRes.getCommunityId());
                }else {
                    Community dbCommunity = communityApiService.get(community.getId());
                    List<RegionInfo> byCityCodeAndType = regionInfoApiService.findByCityCodeAndType(dbCommunity.getCitycode(), RegionType.CITY);
                    String city = "";
                    if (byCityCodeAndType != null && byCityCodeAndType.size() > 0) {
                        city = byCityCodeAndType.get(0).getName();
                    } else {
                        RegionInfo regionInfo = regionInfoApiService.get(community.getAdcode());
                        if (regionInfo != null) {
                            city = regionInfo.getName();
                        }
                    }
                    communityName = city + "-" + community.getName();
                    if (community.getCommunityExtId() != null && community.getCommunityExtId() != 0L) {
                        CommunityExt communityExt = communityCacheApiService.getCommunityExt(community.getCommunityExtId());
                        if (communityExt == null) {
                            logger.error("小区信息未找到, communityId = {}", postInfoRes.getCommunityId());
                        }else {
                            communityName = city + "-" + communityExt.getName();
                        }
                    }
                    address = community.getFullAddress();
                }
                row.add(address);
                row.add(communityName);
                row.add(simpleDateFormat.format(postInfoRes.getCreateTime()));
                row.add(postInfoRes.getContent());
                row.add(postInfoRes.getTitle());
                row.add(postInfoRes.getLikesNumber()+"");
                row.add(postInfoRes.getCommentNumber()+"");
                row.add(postInfoRes.getReadNumber()+"");
                rowList.add(row);
            }
        }
        this.appendExcelData(dataExportRecord,rowList);
        return dataExportRecord;
    }
}
