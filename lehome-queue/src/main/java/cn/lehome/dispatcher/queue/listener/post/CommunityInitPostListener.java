package cn.lehome.dispatcher.queue.listener.post;

import cn.lehome.base.api.business.content.bean.post.*;
import cn.lehome.base.api.business.content.bean.robot.PostMateriel;
import cn.lehome.base.api.business.content.bean.robot.QPostMateriel;
import cn.lehome.base.api.business.content.bean.robot.Robot;
import cn.lehome.base.api.business.content.bean.type.ForumTypeInfo;
import cn.lehome.base.api.business.content.exception.robot.RobotIsNotEnoughException;
import cn.lehome.base.api.business.content.service.post.PostInfoApiService;
import cn.lehome.base.api.business.content.service.post.PostInfoIndexApiService;
import cn.lehome.base.api.business.content.service.robot.PostMaterielApiService;
import cn.lehome.base.api.business.content.service.robot.RobotApiService;
import cn.lehome.base.api.business.content.service.type.ForumTypeInfoApiService;
import cn.lehome.base.api.common.bean.community.Community;
import cn.lehome.base.api.common.component.idgenerator.RedisIdGeneratorComponent;
import cn.lehome.base.api.common.service.community.CommunityApiService;
import cn.lehome.base.api.common.service.community.CommunityCacheApiService;
import cn.lehome.bean.business.content.enums.post.ApprovedStatus;
import cn.lehome.bean.business.content.enums.post.DataStatus;
import cn.lehome.bean.business.content.enums.post.PostType;
import cn.lehome.bean.business.content.enums.post.UserType;
import cn.lehome.bean.common.enums.jnr.JnrTypeEnum;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.compoment.loader.LoaderServiceComponent;
import cn.lehome.framework.base.api.core.compoment.redis.lock.RedisLock;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2018/3/31.
 */
public class CommunityInitPostListener extends AbstractJobListener {

    @Autowired
    private CommunityApiService communityApiService;

    @Autowired
    private PostMaterielApiService postMaterielApiService;

    @Autowired
    private PostInfoIndexApiService postInfoIndexApiService;

    @Autowired
    private ForumTypeInfoApiService forumTypeInfoApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Value("${community.radius}")
    private Double radius;

    @Autowired
    private LoaderServiceComponent loaderServiceComponent;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedisIdGeneratorComponent redisIdGeneratorComponent;

    @Autowired
    private RobotApiService robotApiService;

    @Autowired
    private PostInfoApiService postInfoApiService;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        Long communityId = longEventMessage.getData();
        RedisLock redisLock = new RedisLock(stringRedisTemplate, String.valueOf(communityId), 10l, TimeUnit.SECONDS);
        try {
            if (redisLock.tryLock()) {
                Community community = communityApiService.get(communityId);
                if (community == null) {
                    logger.error("小区信息未找到, communityId = {}", communityId);
                    return;
                }
                if (community.getIsInitPost().equals(YesNoStatus.YES)) {
                    logger.error("小区已经初始化帖子, 不需要再次初始化, communityId = {}", communityId);
                    return;
                }

                List<PostInfo> postInfos = Lists.newArrayList();
                ApiResponse<PostMateriel> response = postMaterielApiService.finAll(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(0, 25));
                int pageTotal = (int) (response.getTotal() / 25) + (response.getTotal() % 25 != 0 ? 1 : 0);
                List<Integer> pages = getRandomPage(pageTotal);
                for (Integer page : pages) {
                    response = postMaterielApiService.finAll(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(page, 25).addOrder(QPostMateriel.contentMd5, PageOrderType.DESC));
                    List<String> md5List = response.getPagedData().stream().map(PostMateriel::getContentMd5).collect(Collectors.toList());
                    String[] str = community.getLocation().split(",");
                    Double longitude = Double.valueOf(str[0]);
                    Double latitude = Double.valueOf(str[1]);
                    List<Long> communityIds = communityCacheApiService.findCommunityIdByGeoRadius(longitude, latitude, radius);
                    if (communityIds.size() == 1) {
                        for (PostMateriel postMateriel : response.getPagedData()) {
                            Robot robot = this.getRobot(communityId);
                            if(robot == null){
                                continue;
                            }
                            PostInfo postInfo = createPostInfo(postMateriel, robot, communityId);
                            if (postInfo != null) {
                                postInfos.add(postInfo);
                            }
                        }
                    } else {
                        Set<String> hasSendPost = Sets.newHashSet();
                        ApiRequest apiRequest = ApiRequest.newInstance().filterLikes(QPostInfoIndex.mdKey, md5List)
                                .filterIn(QPostInfoIndex.communityId, communityIds)
                                .filterLike(QPostInfo.userType, UserType.ROBOT)
                                .filterEqual(QPostInfo.dataStatus, DataStatus.NORMAL);
                        ApiRequestPage requestPage = ApiRequestPage.newInstance().paging(0, 10000);
                        ApiResponse<PostInfoIndex> apiResponse = postInfoIndexApiService.findAll(apiRequest, requestPage);
                        if (!CollectionUtils.isEmpty(apiResponse.getPagedData())) {
                            hasSendPost.addAll(apiResponse.getPagedData().stream().map(PostInfoIndex::getMdKey).collect(Collectors.toList()));
                        }
                        if (apiResponse.getTotal() > 10000L) {
                            while (true) {
                                requestPage.pagingNext();
                                apiResponse = postInfoIndexApiService.findAll(apiRequest, requestPage);

                                if (apiResponse == null || apiResponse.getCount() == 0) {
                                    break;
                                }

                                if (!CollectionUtils.isEmpty(apiResponse.getPagedData())) {
                                    hasSendPost.addAll(apiResponse.getPagedData().stream().map(PostInfoIndex::getMdKey).collect(Collectors.toList()));
                                }

                                if (apiResponse.getCount() < requestPage.getPageSize()) {
                                    break;
                                }
                            }
                        }
                        for (PostMateriel postMateriel : response.getPagedData()) {
                            if (!hasSendPost.contains(postMateriel.getContentMd5())) {
                                Robot robot = this.getRobot(communityId);
                                if(robot == null){
                                    continue;
                                }
                                PostInfo postInfo = createPostInfo(postMateriel, robot, communityId);
                                if (postInfo != null) {
                                    postInfos.add(postInfo);
                                }
                            }
                        }
                    }
                    if (postInfos.size() == 25) {
                        break;
                    }
                }
                if (postInfos.size() == 0) {
                    logger.error("没有可以发的物料, communityId = {}", communityId);
                } else {
                    for (PostInfo postInfo : postInfos) {
                        postInfoApiService.save(postInfo);
                    }
                }
                communityApiService.finishInitPost(communityId);
            }
        }finally {
            redisLock.unlock();
        }
    }

    private PostInfo createPostInfo(PostMateriel postMateriel, Robot robot, Long communityId) {
        PostInfo postInfo = new PostInfo();
        ForumTypeInfo forumTypeInfo = forumTypeInfoApiService.findOne(postMateriel.getTypeId().intValue());
        if (forumTypeInfo == null) {
            return null;
        }
        String postId = redisIdGeneratorComponent.generateByOrderId(forumTypeInfo.getActivityId(), JnrTypeEnum.SEQ_T_NEIGHBOR_POST);
        loaderServiceComponent.load(postMateriel, QPostMateriel.picList);
        postInfo.setActivityId(forumTypeInfo.getActivityId());
        postInfo.setPostType(PostType.LT);
        postInfo.setTitle(forumTypeInfo.getType());
        postInfo.setUserId(robot.getId());
        postInfo.setUserType(UserType.ROBOT);
        postInfo.setCommunityId(communityId.toString());
        postInfo.setPostId(postId);
        postInfo.setMdKey(postMateriel.getContentMd5());
        postInfo.setIsAnonymous(YesNoStatus.NO);
        postInfo.setContent(postMateriel.getContent());
        postInfo.setApprovedStatus(ApprovedStatus.UNAPPROVED);
        postInfo.setDataStatus(DataStatus.NORMAL);
        postInfo.setTopStatus(YesNoStatus.NO);
        postInfo.setForumDisplayStatus(YesNoStatus.YES);
        postInfo.setMblNo("");
        if (!CollectionUtils.isEmpty(postMateriel.getPicList())) {
            List<PicInfo> picInfos = Lists.newArrayList();
            Integer i = 0;
            for (String url : postMateriel.getPicList()) {
                i += 1;
                PicInfo picInfo = new PicInfo();
                picInfo.setPicUrl(url);
                picInfo.setPostId(postId);
                picInfo.setPicId(i.toString());
                picInfos.add(picInfo);
            }
            postInfo.setPicInfoList(picInfos);
        }
        return postInfo;
    }

    private List<Integer> getRandomPage(int pageTotal) {
        List<Integer> pages = Lists.newArrayList();
        Random random = new Random();
        if (pageTotal <= 3) {
            for (int i = 0; i < pageTotal; i++) {
                pages.add(i);
            }
        }

        for (int i = 0; i < 10; i++) {
            int page = random.nextInt(pageTotal);
            if (!pages.contains(page)) {
                pages.add(page);
                if (pages.size() == 3) {
                    break;
                }
            }
        }
        return pages;
    }

    private Robot getRobot(Long communityId){
        Robot robot = null;
        try {
            robot = robotApiService.randomRobotByCommunity(communityId);
        } catch (RobotIsNotEnoughException e) {
            logger.error("机器人不足");
        }
        return robot;
    }

    @Override
    public String getConsumerId() {
        return "community_init_post";
    }
}
