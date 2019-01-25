package cn.lehome.dispatcher.queue.listener.post;

import cn.lehome.base.api.content.bean.extension.ExtensionDetailInfo;
import cn.lehome.base.api.content.bean.extension.ExtensionInfo;
import cn.lehome.base.api.content.bean.extension.ExtensionInfoIndex;
import cn.lehome.base.api.content.bean.post.PicInfo;
import cn.lehome.base.api.content.bean.post.PostInfo;
import cn.lehome.base.api.content.bean.post.TempPostExtensionInfo;
import cn.lehome.base.api.content.bean.post.TempPostInfo;
import cn.lehome.base.api.content.bean.robot.Robot;
import cn.lehome.base.api.content.bean.type.ForumTypeInfo;
import cn.lehome.base.api.content.service.extension.ExtensionInfoApiService;
import cn.lehome.base.api.content.service.extension.ExtensionInfoIndexApiService;
import cn.lehome.base.api.content.service.post.PostInfoApiService;
import cn.lehome.base.api.content.service.post.TempPostExtensionInfoApiService;
import cn.lehome.base.api.content.service.post.TempPostInfoApiService;
import cn.lehome.base.api.content.service.robot.RobotApiService;
import cn.lehome.base.api.content.service.type.ForumTypeInfoApiService;
import cn.lehome.base.api.tool.bean.community.Community;
import cn.lehome.base.api.tool.bean.community.QCommunity;
import cn.lehome.base.api.tool.bean.region.RegionInfo;
import cn.lehome.base.api.tool.compoment.idgenerator.RedisIdGeneratorComponent;
import cn.lehome.base.api.tool.service.community.CommunityApiService;
import cn.lehome.base.api.tool.service.community.CommunityCacheApiService;
import cn.lehome.base.api.tool.service.region.RegionInfoApiService;
import cn.lehome.bean.content.entity.enums.post.*;
import cn.lehome.bean.tool.entity.enums.jnr.JnrTypeEnum;
import cn.lehome.bean.tool.entity.enums.region.RegionType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import cn.lehome.framework.base.api.core.util.MD5Util;
import cn.lehome.framework.bean.core.enums.SendStatus;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class RobotPostListener extends AbstractJobListener {

    @Autowired
    private TempPostInfoApiService tempPostInfoApiService;

    @Autowired
    private RegionInfoApiService regionInfoApiService;

    @Autowired
    private CommunityApiService communityApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    private static final Long country = 100000L;

    @Autowired
    private RobotApiService robotApiService;

    @Autowired
    private RedisIdGeneratorComponent redisIdGeneratorComponent;

    @Autowired
    private ForumTypeInfoApiService forumTypeInfoApiService;

    @Autowired
    private ExtensionInfoIndexApiService extensionInfoIndexApiService;

    @Autowired
    private PostInfoApiService postInfoApiService;

    @Autowired
    private ExtensionInfoApiService extensionInfoApiService;

    @Autowired
    private TempPostExtensionInfoApiService tempPostExtensionInfoApiService;
    @Value("${community.radius}")
    private Double radius;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        TempPostInfo tempPostInfo = tempPostInfoApiService.get(longEventMessage.getData());
        if (tempPostInfo == null) {
            logger.error("机器人发帖任务未找到, id = {}", longEventMessage.getData());
            return;
        }
        tempPostInfoApiService.updateStatus(longEventMessage.getData(), SendStatus.DEALING);
        boolean isDeal = true;
        try {
            String md5 = MD5Util.encoderByMd5(tempPostInfo.getContent());
            Set<Long> communityIds = findAllCommunity(tempPostInfo);
            List<Long> sendCommunityIds = Lists.newArrayList();
            for (Long communityId : communityIds) {
                if (sendCommunityIds.contains(communityId)) {
                    logger.error("周边小区或本小区已经发过该贴不用再发");
                    continue;
                }
                Community community = communityCacheApiService.getCommunity(communityId);
                String[] str = community.getLocation().split(",");
                Double longitude = Double.valueOf(str[0]);
                Double latitude = Double.valueOf(str[1]);
                List<Long> aroudCommunityIds = communityCacheApiService.findCommunityIdByGeoRadius(longitude, latitude, radius);
                logger.error("周围小区ID集合 ids = {}, radius = {}", StringUtils.join(aroudCommunityIds, ","), radius);
                Robot robot;
                try {
                    if (tempPostInfo.getRobotId() != null && tempPostInfo.getRobotId() != 0L) {
                        robot = robotApiService.get(tempPostInfo.getRobotId());
                    } else {
                        robot = robotApiService.randomRobotByCommunity(communityId);
                    }
                    if (robot == null) {
                        logger.error("机器人未找到");
                        continue;
                    }
                    this.createPost(tempPostInfo, robot, communityId, md5);
                    sendCommunityIds.addAll(aroudCommunityIds);
                } catch (Exception e) {
                    logger.error("查找机器人失败", e);
                    continue;
                }

            }

        } catch (Exception e) {
            logger.error("机器人发帖异常", e);
            isDeal = false;
        }
        if (isDeal) {
            tempPostInfoApiService.updateStatus(longEventMessage.getData(), SendStatus.DEAL);
        } else {
            tempPostInfoApiService.updateStatus(longEventMessage.getData(), SendStatus.DEAL_FAILED);
        }




    }

    private void createPost(TempPostInfo tempPostInfo, Robot robot, Long communityId, String md5) {
        String title = "";
        if (tempPostInfo.getPostType().equals(PostType.CRN)) {
            ExtensionInfoIndex extensionInfoIndex = extensionInfoIndexApiService.get(tempPostInfo.getActivityId(), false);
            if (extensionInfoIndex != null) {
                title = extensionInfoIndex.getTitle();
            }
        } else {
            ForumTypeInfo forumTypeInfo = forumTypeInfoApiService.findByActivityId(tempPostInfo.getActivityId());
            if (forumTypeInfo != null) {
                title = forumTypeInfo.getType();
            }
        }
        String postId = redisIdGeneratorComponent.generateByOrderId(tempPostInfo.getActivityId(), JnrTypeEnum.SEQ_T_NEIGHBOR_POST);
        PostInfo postInfo = new PostInfo();
        postInfo.setUserId(robot.getId());
        postInfo.setMblNo("");
        postInfo.setUserType(UserType.ROBOT);
        postInfo.setIsAnonymous(tempPostInfo.getIsAnonymous());
        postInfo.setMdKey(md5);
        postInfo.setTitle(title);
        postInfo.setCommunityId(communityId.toString());
        postInfo.setPostId(postId);
        postInfo.setPostType(tempPostInfo.getPostType());
        postInfo.setActivityId(tempPostInfo.getActivityId());
        postInfo.setContent(tempPostInfo.getContent());
        if(tempPostInfo.getPostType().equals(PostType.LT)){
            postInfo.setForumDisplayStatus(YesNoStatus.YES);
        }
        List<PicInfo> picInfos = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(tempPostInfo.getPicList())) {
            for (int i = 0; i < tempPostInfo.getPicList().size(); i++) {
                PicInfo picInfo = new PicInfo();
                picInfo.setPicId(String.valueOf(i));
                picInfo.setPostId(postInfo.getPostId());
                picInfo.setPicUrl(tempPostInfo.getPicList().get(i));
                picInfo.setPicSts(DataStatus.NORMAL);
                picInfo.setCompresspicUrl("");
                picInfos.add(picInfo);
            }
        }
        postInfo.setPicInfoList(picInfos);
        postInfoApiService.save(postInfo);
        robotApiService.updatePostNumber(robot.getId());
        if(tempPostInfo.getSendHomePageStatus().equals(YesNoStatus.YES)){
            createExtensionInfo(tempPostInfo,postId);
        }
    }

    private void createExtensionInfo(TempPostInfo tempPostInfo,String postId) {
        TempPostExtensionInfo tempPostExtensionInfo =  tempPostExtensionInfoApiService.findByTempPostId(tempPostInfo.getId());
        ExtensionInfo extensionInfo =  BeanMapping.map(tempPostExtensionInfo, ExtensionInfo.class);
        if(!CollectionUtils.isEmpty(tempPostExtensionInfo.getCoverImgUrls())){
            extensionInfo.setPicList(tempPostExtensionInfo.getCoverImgUrls());
        }
        if(!CollectionUtils.isEmpty(tempPostExtensionInfo.getExtensionRegionIdList())){
            extensionInfo.setRegionIdList(tempPostExtensionInfo.getExtensionRegionIdList());
        }
        String activityId = redisIdGeneratorComponent.generateByOrderId("00000", JnrTypeEnum.SEQ_T_NEIGHBOR_EXTENSION);
        extensionInfo.setActivityId(activityId);
        extensionInfo.setSourceType(ExtensionSourceType.FORUM_TO_HOMEPAGE);
        extensionInfo.setTag(TagType.CRN);

        ExtensionDetailInfo extensionDetailInfo =  BeanMapping.map(tempPostExtensionInfo,ExtensionDetailInfo.class);
        extensionDetailInfo.setActivityId(activityId);
        extensionDetailInfo.setPostId(postId);
        extensionInfo.setExtensionDetailInfo(extensionDetailInfo);
        extensionInfoApiService.save(extensionInfo);
        postInfoApiService.updatePushIndex(postId,YesNoStatus.YES);
    }

    private Set<Long> findAllCommunity(TempPostInfo tempPostInfo) {
        Set<Long> communityIds = Sets.newHashSet();
        if (!CollectionUtils.isEmpty(tempPostInfo.getCommunityIdList())) {
            communityIds.addAll(tempPostInfo.getCommunityIdList());
        }
        if (!CollectionUtils.isEmpty(tempPostInfo.getRegionIdList())) {
            if (tempPostInfo.getRegionIdList().contains(country)) {
                List<Community> communities = communityApiService.findAll(ApiRequest.newInstance());
                List<Long> ids = communities.stream().map(Community::getId).collect(Collectors.toList());
                communityIds.addAll(ids);
            } else {
                Map<Long, RegionInfo> map = regionInfoApiService.findAll(tempPostInfo.getRegionIdList());
                Set<Long> pcode = map.values().stream().filter(regionInfo -> regionInfo.getType().equals(RegionType.PROVINCE)).map(RegionInfo::getId).collect(Collectors.toSet());
                Set<String> cityCode = map.values().stream().filter(regionInfo -> regionInfo.getType().equals(RegionType.CITY)).map(RegionInfo::getCityCode).collect(Collectors.toSet());
                Set<Long> acode = map.values().stream().filter(regionInfo -> regionInfo.getType().equals(RegionType.DISTRICT)).map(RegionInfo::getId).collect(Collectors.toSet());
                if (!CollectionUtils.isEmpty(pcode)) {
                    List<Community> communities = communityApiService.findAll(ApiRequest.newInstance().filterIn(QCommunity.pcode, pcode));
                    if (!CollectionUtils.isEmpty(communities)) {
                        List<Long> ids = communities.stream().map(Community::getId).collect(Collectors.toList());
                        communityIds.addAll(ids);
                    }
                }
                if (!CollectionUtils.isEmpty(cityCode)) {
                    List<Community> communities = communityApiService.findAll(ApiRequest.newInstance().filterIn(QCommunity.citycode, cityCode));
                    if (!CollectionUtils.isEmpty(communities)) {
                        List<Long> ids = communities.stream().map(Community::getId).collect(Collectors.toList());
                        communityIds.addAll(ids);
                    }
                }
                if (!CollectionUtils.isEmpty(acode)) {
                    List<Community> communities = communityApiService.findAll(ApiRequest.newInstance().filterIn(QCommunity.adcode, acode));
                    if (!CollectionUtils.isEmpty(communities)) {
                        List<Long> ids = communities.stream().map(Community::getId).collect(Collectors.toList());
                        communityIds.addAll(ids);
                    }
                }
            }

        }
        return communityIds;
    }


    @Override
    public String getConsumerId() {
        return "robot_post";
    }
}
