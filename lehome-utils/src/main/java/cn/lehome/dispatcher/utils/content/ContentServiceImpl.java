package cn.lehome.dispatcher.utils.content;

import cn.lehome.base.api.content.bean.comment.CommentInfo;
import cn.lehome.base.api.content.bean.comment.QCommentInfo;
import cn.lehome.base.api.content.bean.extension.ExtensionDetailInfo;
import cn.lehome.base.api.content.bean.extension.ExtensionInfo;
import cn.lehome.base.api.content.bean.extension.QExtensionDetailInfo;
import cn.lehome.base.api.content.bean.extension.QExtensionInfo;
import cn.lehome.base.api.content.bean.post.*;
import cn.lehome.base.api.content.service.comment.CommentInfoApiService;
import cn.lehome.base.api.content.service.comment.CommentInfoIndexApiService;
import cn.lehome.base.api.content.service.extension.*;
import cn.lehome.base.api.content.service.post.*;
import cn.lehome.base.api.tool.bean.community.Community;
import cn.lehome.base.api.tool.bean.job.ScheduleJob;
import cn.lehome.base.api.tool.compoment.idgenerator.RedisIdGeneratorComponent;
import cn.lehome.base.api.tool.service.community.CommunityApiService;
import cn.lehome.base.api.tool.service.community.CommunityCacheApiService;
import cn.lehome.base.api.tool.service.job.ScheduleJobApiService;
import cn.lehome.bean.content.entity.enums.post.*;
import cn.lehome.bean.content.entity.search.comment.CommentInfoIndexEntity;
import cn.lehome.bean.content.entity.search.likes.LikesInfoIndexEntity;
import cn.lehome.bean.content.entity.search.post.PostInfoIndexEntity;
import cn.lehome.bean.tool.entity.enums.jnr.JnrTypeEnum;
import cn.lehome.dispatcher.utils.es.util.EsFlushUtil;
import cn.lehome.dispatcher.utils.es.util.EsScrollResponse;
import cn.lehome.framework.base.api.core.compoment.request.ApiPageRequestHelper;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.BeanMapping;
import cn.lehome.framework.base.api.core.util.ExcelUtils;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

/**
 * Created by zuoguodong on 2018/4/3
 */
@Service("contentService")
public class ContentServiceImpl implements ContentService{

    @Autowired
    private CommentInfoApiService commentInfoApiService;

    @Autowired
    private CommentInfoIndexApiService commentInfoIndexApiService;

    @Autowired
    private PostInfoApiService postInfoApiService;

    @Autowired
    private PicInfoApiService picInfoApiService;

    @Autowired
    private LikesInfoApiService likesInfoApiService;

    @Autowired
    private LikesInfoIndexApiService likesInfoIndexApiService;

    @Autowired
    private PostInfoIndexApiService postInfoIndexApiService;

    @Autowired
    private ExtensionInfoApiService extensionInfoApiService;

    @Autowired
    private ExtensionPicInfoApiService extensionPicInfoApiService;

    @Autowired
    private ExtensionDetailInfoApiService extensionDetailInfoApiService;

    @Autowired
    private PowerInfoApiService powerInfoApiService;

    @Autowired
    private ExtensionInfoIndexApiService extensionInfoIndexApiService;

    @Autowired
    private ThreadPoolExecutor userTaskThreadPool;

    @Autowired
    private CommunityApiService communityApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private RedisIdGeneratorComponent redisIdGeneratorComponent;

    @Autowired
    private ScheduleJobApiService scheduleJobApiService;

    @Value("${extension.type.id}")
    private Integer extensionTypeId;


    @Override
    public void initContentIndex(String input[]) {
        if(input.length < 2){
            System.out.println("参数错误");
            return;
        }
        String option = input[1];
        int pageNo = 0;
        if(input.length > 2){
            pageNo = Integer.valueOf(input[2]);
        }
        if("initCommentIndex".equals(option)) {
           //this.initCommentIndex(pageNo);
            initCommentIndexForEsClient(pageNo);
        }else if("initPostIndex".equals(option)) {
            this.initPostIndex(pageNo);
            //initPostIndexForEsClient(pageNo);
        }else if("initLikesIndex".equals(option)) {
           // this.initLikesIndex(pageNo);
            initLikesIndexForEsClient(pageNo);
        }else if("initExtensionIndex".equals(option)) {
            this.initExtensionIndex(pageNo);
        }else{
            System.out.println("参数错误");
        }
    }

    @Override
    public void updatePostInfoIndexByMd5Key(String input[]) {
        int pageSize = 100;
        int count = 0;
        int pageIndex = 0;
        if(input.length < 2){
            System.out.println("参数错误");
            return;
        }
        String md5Key = input[1];
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiRequest apiRequest = ApiRequest.newInstance();
        apiRequest.filterEqual(QPostInfo.mdKey,md5Key);
        ApiResponse<PostInfo> response = postInfoApiService.findAll(apiRequest,apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            Collection<PostInfo> collection = response.getPagedData();
            collection.forEach(c -> {
                List<PicInfo> picList = picInfoApiService.findByPostId(c.getPostId());
                c.setPicInfoList(picList);
                postInfoIndexApiService.save(c);
            });
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = postInfoApiService.findAll(apiRequest,apiRequestPage);
        }
        System.out.println("initPostIndex 数据处理完毕 " + count);
    }


    private void initCommentIndex(int pageIndex){
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<CommentInfo> response = commentInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            final Collection<CommentInfo> collection = response.getPagedData();
            userTaskThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    collection.forEach(l->commentInfoIndexApiService.save(l));
                }
            });
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = commentInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        while (userTaskThreadPool.getActiveCount() != 0) {
            try {
                System.out.println("initCommentIndex 数据加载完毕" + count + "，还有" + userTaskThreadPool.getQueue().size() + "任务等执行");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("任务执行完毕");
    }

    private void initCommentIndexForEsClient(int pageIndex){
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<CommentInfo> response = commentInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            try {
                List<CommentInfoIndexEntity> commentInfoIndexEntities = BeanMapping.mapList(Lists.newArrayList(response.getPagedData()), CommentInfoIndexEntity.class);
                commentInfoIndexEntities.forEach(item->item.setId(item.getCommentId()));
                EsFlushUtil.getInstance().batchInsert(commentInfoIndexEntities);
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = commentInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("任务执行完毕");
    }

    private void initPostIndex(int pageIndex){
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<PostInfo> response = postInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            final Collection<PostInfo> collection = response.getPagedData();
            userTaskThreadPool.execute(() -> collection.forEach(c -> {
                List<PicInfo> picList = picInfoApiService.findByPostId(c.getPostId());
                c.setPicInfoList(picList);
                postInfoIndexApiService.save(c);
            }));
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = postInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
            System.out.println(pageIndex);
        }
        while (userTaskThreadPool.getActiveCount() != 0) {
            try {
                System.out.println("initPostIndex 数据加载完毕" + count + "，还有" + userTaskThreadPool.getQueue().size() + "任务等执行");
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("initPostIndex 数据处理完毕 " + count);
    }

    private void initPostIndexForEsClient(int pageIndex){
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<PostInfo> response = postInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            try {
                List<PostInfoIndexEntity> postInfoIndexEntities = BeanMapping.mapList(Lists.newArrayList(response.getPagedData()), PostInfoIndexEntity.class);

                //正常话题下的帖子分类
                Set<String> activityIdSet = postInfoIndexEntities.stream().filter(postInfoIndexEntity -> YesNoStatus.NO.equals(postInfoIndexEntity.getPushIndex())).map(PostInfoIndexEntity::getActivityId).collect(Collectors.toSet());

                ApiRequest apiRequestExtension =ApiRequest.newInstance();
                apiRequestExtension.filterIn(QExtensionInfo.activityId, activityIdSet);
                List<ExtensionInfo> extensionInfoList = extensionInfoApiService.findAll(apiRequestExtension);

                Map<String,Integer> activityIdTypeIdMap = Maps.newHashMap();
                if(!CollectionUtils.isEmpty(extensionInfoList)) {
                    extensionInfoList.forEach(extensionInfo -> activityIdTypeIdMap.put(extensionInfo.getActivityId(),extensionInfo.getTypeId()));
                }

                //上首页的帖子id(对应extension_detail_info中的activityId)
                Set<String> pushPostIdSet = postInfoIndexEntities.stream().filter(postInfoIndexEntity -> YesNoStatus.YES.equals(postInfoIndexEntity.getPushIndex())).map(PostInfoIndexEntity::getPostId).collect(Collectors.toSet());

                //用postId找detail
                List<ExtensionDetailInfo> extensionDetailInfoList = Lists.newArrayList();
                if(!CollectionUtils.isEmpty(pushPostIdSet)) {
                    ApiRequest apiRequestDetail = ApiRequest.newInstance();
                    apiRequestDetail.filterIn(QExtensionDetailInfo.postId, pushPostIdSet);
                    extensionDetailInfoList = extensionDetailInfoApiService.findAll(apiRequestDetail);

                }
                Map<String,Integer> pushedPostIdTypeIdMap = Maps.newHashMap();
                if(!CollectionUtils.isEmpty(extensionDetailInfoList)) {
                    //取出detail中的 activityId
                    Set<String> pushedActivityIdSet = extensionDetailInfoList.stream().map(ExtensionDetailInfo::getActivityId).collect(Collectors.toSet());

                    //用activityId去查typeId
                    ApiRequest apiRequestPushedExtension =ApiRequest.newInstance();
                    apiRequestPushedExtension.filterIn(QExtensionInfo.activityId, pushedActivityIdSet);
                    List<ExtensionInfo> pushedExtensionInfoList = extensionInfoApiService.findAll(apiRequestPushedExtension);

                    if(!CollectionUtils.isEmpty(pushedExtensionInfoList)) {
                        for(ExtensionDetailInfo detailInfo : extensionDetailInfoList) {
                            pushedExtensionInfoList.forEach(extensionInfo -> {
                                if(extensionInfo.getActivityId().equals(detailInfo.getActivityId())) {
                                    pushedPostIdTypeIdMap.put(detailInfo.getPostId(), extensionInfo.getTypeId());
                                }
                            });
                        }
                    }
                }

                for(PostInfoIndexEntity entity: postInfoIndexEntities) {
                    Integer typeId = 0;
                    if(YesNoStatus.NO.equals(entity.getPushIndex())) {
                         typeId = activityIdTypeIdMap.get(entity.getActivityId());
                    }else{
                         typeId = pushedPostIdTypeIdMap.get(entity.getPostId());
                    }
                    entity.setId(entity.getPostId());
                    entity.setTypeId(typeId == null ? 0 : typeId);

                }
                EsFlushUtil.getInstance().batchInsert(postInfoIndexEntities);
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = postInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("任务执行完毕");
    }

    private void initExtensionIndex(int pageIndex){
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<ExtensionInfo> response = extensionInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            Collection<ExtensionInfo> collection = response.getPagedData();
            collection.forEach(c -> {
                ExtensionDetailInfo extensionDetailInfo = extensionDetailInfoApiService.findOne(c.getActivityId());
                c.setExtensionDetailInfo(extensionDetailInfo);
                List<String> picList = extensionPicInfoApiService.findByActivity(c.getActivityId());
                c.setPicList(picList);
                List<Long> powerInfoIdList = powerInfoApiService.findByActivityId(c.getActivityId());
                c.setRegionIdList(powerInfoIdList);
                extensionInfoIndexApiService.saveOrUpdate(c);
            });
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = extensionInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("initExtensionIndex 数据处理完毕 " + count);
    }

    private void initLikesIndex(int pageIndex){
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<LikesInfo> response = likesInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            Collection<LikesInfo> collection = response.getPagedData();
            collection.forEach(likesInfo -> {
                likesInfoIndexApiService.save(likesInfo);
            });
            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = likesInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("initLikesIndex 数据处理完毕 " + count);
    }


    private void initLikesIndexForEsClient(int pageIndex){
        int pageSize = 100;
        int count = 0;
        ApiRequestPage apiRequestPage = ApiRequestPage.newInstance();
        apiRequestPage.paging(pageIndex,pageSize);
        ApiResponse<LikesInfo> response = likesInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        while(response.getPagedData().size()>0){
            count += response.getPagedData().size();
            try {
                List<LikesInfoIndexEntity> likesInfoIndexEntities = BeanMapping.mapList(Lists.newArrayList(response.getPagedData()), LikesInfoIndexEntity.class);
                likesInfoIndexEntities.forEach(item->item.setId(item.getLikesId()));
                EsFlushUtil.getInstance().batchInsert(likesInfoIndexEntities);
            }catch (Exception e){
                e.printStackTrace();
                continue;
            }

            pageIndex++;
            apiRequestPage.paging(pageIndex,pageSize);
            response = likesInfoApiService.findAll(ApiRequest.newInstance(),apiRequestPage);
        }
        System.out.println("initLikesIndex 数据处理完毕 " + count);
    }
    @Override
    public void deleteRepeatPost(){
        ApiRequest request = ApiRequest.newInstance();
        List<Community> list = communityApiService.findAll(request);
        list.forEach(community -> {
            try {
                System.out.println("communityId" + community.getId());
                String[] str = community.getLocation().split(",");
                Double longitude = Double.valueOf(str[0]);
                Double latitude = Double.valueOf(str[1]);
                List<Long> communityIds = communityCacheApiService.findCommunityIdByGeoRadius(longitude, latitude, 6000.0);
                ApiRequest apiRequest = ApiRequest.newInstance()
                        .filterIn(QPostInfoIndex.communityId, communityIds)
                        .filterLike(QPostInfoIndex.userType, UserType.ROBOT)
                        .filterLike(QPostInfoIndex.dataStatus, DataStatus.NORMAL);
                ApiResponse<PostInfoIndex> response = postInfoIndexApiService.findAll(apiRequest, ApiRequestPage.newInstance().paging(0, 10000));
                Collection<PostInfoIndex> postInfoList = response.getPagedData();
                Collection<String> collection = this.distinct(postInfoList);
                postInfoList.forEach(postInfoIndex -> {
                    if (!collection.contains(postInfoIndex.getPostId())) {
                        System.out.println(" -> delete [communityId:" + postInfoIndex.getCommunityId() + "][postId:" + postInfoIndex.getPostId() + "]");
                        try {
                            postInfoApiService.delete(postInfoIndex.getPostId(),YesNoStatus.NO);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }catch(Exception e){
                e.printStackTrace();
            }
        });
        System.out.println("数据处理完毕 " + list.size());
    }

    private Collection<String> distinct(Collection<PostInfoIndex> postInfoList){
        Map<String,String> map = new HashMap<>();
        postInfoList.forEach(postInfoIndex -> map.put(postInfoIndex.getMdKey(),postInfoIndex.getPostId()));
        return map.values();
    }


    @Override
    public void coverExtensionToPost(String[] input) {
        String file = input[1];
        int sheetIndex = Integer.valueOf(input[2]);
            try {
                ExcelUtils excelUtils = new ExcelUtils(file);
                List<List<String>> data = excelUtils.read(sheetIndex);
                for (List<String>  rowList : data){
                    if(rowList.size()<2){
                        continue;
                    }
                    String activityId="";
                    String forumPostId = "";
                    try {
                        activityId = rowList.get(0).trim();
                    }catch (Exception e){
                        System.out.println(rowList.get(0)+" : "+e.getMessage());
                        continue;
                    }
                    if(null == rowList.get(1) || "null".equals(rowList.get(1))){
                        continue;
                    }
                    try {
                        forumPostId = rowList.get(1).trim();
                    }catch (Exception e){
                        System.out.println(rowList.get(1) +" : "+e.getMessage());
                        continue;
                    }
                    System.out.println("activityId 为: "+activityId + "  postId 为: "+forumPostId);
                    extensionInfoApiService.makeRelation(activityId,forumPostId, ExtensionSourceType.POST_TO_EXTENSION);
                    postInfoApiService.updatePushIndex(forumPostId, YesNoStatus.YES);
                    List<PostInfo>  postInfos = postInfoApiService.findByActivityIdAndDataStatus(activityId);
                    PostInfo forumUserPostInfo = postInfoApiService.findOne(forumPostId);
                    System.out.println("postinfos JSON:  "+JSON.toJSON(postInfos));
                    if(!CollectionUtils.isEmpty(postInfos)){
                        System.out.println("评论数据转换开始...");
                        for(PostInfo postInfo :postInfos){
                            CommentInfo commentInfo = new CommentInfo();
                            commentInfo.setPostId(forumUserPostInfo.getPostId());
                            commentInfo.setCommentContent(postInfo.getContent());
                            commentInfo.setCommentUserId(postInfo.getUserId());
                            commentInfo.setCommentUserType(postInfo.getUserType());
                            commentInfo.setBeCommentId("");
                            commentInfo.setPostType(PostType.LT);
                            commentInfo.setBeReplyId("");
                            commentInfo.setCommentType(CommentType.COMMENT);
                            commentInfo.setBeReplyUserId(forumUserPostInfo.getUserId());
                            commentInfo.setBeReplyUserType(forumUserPostInfo.getUserType());
                            commentInfo.setCommentId(redisIdGeneratorComponent.generateByOrderId(postInfo.getPostId(), JnrTypeEnum.SEQ_T_NEIGHBOR_COMMENT));
                            commentInfoApiService.save(commentInfo);
                            ApiRequest commentApiRequest = ApiRequest.newInstance().filterEqual(QCommentInfo.postId,postInfo.getPostId()).filterEqual(QCommentInfo.dataStatus, DataStatus.NORMAL);
                            List<CommentInfo> commentInfos = ApiPageRequestHelper.request(commentApiRequest,ApiRequestPage.newInstance().paging(0,100).addOrder(QCommentInfo.createTime, PageOrderType.ASC),commentInfoApiService::findAll);
                            System.out.println("commentInfos JSON: "+JSON.toJSON(commentInfos));
                            if(!CollectionUtils.isEmpty(commentInfos)){
                                System.out.println("回复数据转换开始...");
                                Map<String, String> oldToNewMap = Maps.newHashMap();
                                List<CommentInfo> replyList = Lists.newArrayList();
                                for(CommentInfo extCommentInfo :commentInfos) {
                                    if (extCommentInfo.getCommentType().equals(CommentType.COMMENT)) {
                                        CommentInfo replyInfo = new CommentInfo();
                                        replyInfo.setPostId(forumUserPostInfo.getPostId());
                                        replyInfo.setCommentContent(extCommentInfo.getCommentContent());
                                        replyInfo.setBeCommentId(commentInfo.getCommentId());
                                        replyInfo.setCommentUserId(extCommentInfo.getCommentUserId());
                                        replyInfo.setBeReplyId(commentInfo.getCommentId());
                                        replyInfo.setPostType(extCommentInfo.getPostType());
                                        replyInfo.setCommentUserType(commentInfo.getCommentUserType());
                                        replyInfo.setCommentType(CommentType.REPLY);
                                        replyInfo.setBeReplyUserId(extCommentInfo.getBeReplyUserId());
                                        replyInfo.setBeReplyUserType(extCommentInfo.getBeReplyUserType());
                                        replyInfo.setCommentId(redisIdGeneratorComponent.generateByOrderId(commentInfo.getPostId(), JnrTypeEnum.SEQ_T_NEIGHBOR_COMMENT));
                                        commentInfoApiService.save(replyInfo);
                                        oldToNewMap.put(extCommentInfo.getCommentId(), replyInfo.getCommentId());
                                    } else {
                                        replyList.add(extCommentInfo);
                                    }
                                }

                                for (CommentInfo extCommentInfo : replyList) {
                                    CommentInfo replyInfo = new CommentInfo();
                                    replyInfo.setPostId(forumUserPostInfo.getPostId());
                                    replyInfo.setCommentContent(extCommentInfo.getCommentContent());
                                    replyInfo.setBeCommentId(commentInfo.getCommentId());
                                    replyInfo.setCommentUserId(extCommentInfo.getCommentUserId());
                                    String newBeRelyId = oldToNewMap.get(extCommentInfo.getBeReplyId());
                                    replyInfo.setBeReplyId(newBeRelyId);
                                    replyInfo.setPostType(extCommentInfo.getPostType());
                                    replyInfo.setCommentUserType(commentInfo.getCommentUserType());
                                    replyInfo.setCommentType(CommentType.REPLY);
                                    replyInfo.setBeReplyUserId(extCommentInfo.getBeReplyUserId());
                                    replyInfo.setBeReplyUserType(extCommentInfo.getBeReplyUserType());
                                    replyInfo.setCommentId(redisIdGeneratorComponent.generateByOrderId(commentInfo.getPostId(), JnrTypeEnum.SEQ_T_NEIGHBOR_COMMENT));
                                    commentInfoApiService.save(replyInfo);
                                    oldToNewMap.put(extCommentInfo.getCommentId(), replyInfo.getCommentId());
                                }
                            }
                        }
                    }

                }
            } catch (Exception e) {
                System.out.println("数据处理出错：" + e.getMessage());
                e.printStackTrace();
                return;
            }
        System.out.println("数据处理完毕");
    }

    @Override
    public void repairExtensionRelationWithType(String[] input) {
        System.out.println("热门凑热闹标签ID为: "+extensionTypeId);
        String file = input[1];
        int sheetIndex = Integer.valueOf(input[2]);
        int count = 0;
        try {
            ExcelUtils excelUtils = new ExcelUtils(file);
            List<List<String>> data = excelUtils.read(sheetIndex);
            for (List<String>  rowList : data){
                if(rowList.size()<1){
                    continue;
                }
                String activityId="";
                try {
                    activityId = rowList.get(0).trim();
                }catch (Exception e){
                    System.out.println(rowList.get(0)+" : "+e.getMessage());
                    continue;
                }
                extensionInfoApiService.updateTypeIdByActivityId(activityId,extensionTypeId);
                count++;
            }
        } catch (Exception e) {
            System.out.println("数据处理出错：" + e.getMessage());
            e.printStackTrace();
            return;
        }
        System.out.println("修复extension数据完毕,共修复:"+count+"条");
    }

    @Override
    public void createCancelTopPostTask() {
        ApiRequest apiRequest  = ApiRequest.newInstance().filterLike(QPostInfoIndex.dataStatus,DataStatus.NORMAL).filterLike(QPostInfoIndex.topStatus,YesNoStatus.YES);
        ApiResponse<PostInfoIndex> apiResponse = postInfoIndexApiService.findAll(apiRequest,ApiRequestPage.newInstance().paging(0,1000));
        List<PostInfoIndex> postInfoIndices = Lists.newArrayList(apiResponse.getPagedData());
        if (!CollectionUtils.isEmpty(postInfoIndices)){
            System.out.println("置顶帖子总共 : "+apiResponse.getCount()+"条");
            List<ScheduleJob> scheduleJobList = Lists.newArrayList();
            for (PostInfoIndex postInfoIndex: postInfoIndices){
                postInfoApiService.updateTopStatus(postInfoIndex.getPostId(),YesNoStatus.YES);
            }
            System.out.println("刷新置顶帖子取消置顶时间数据完成 共"+scheduleJobList.size()+"条");
        }
    }

    @Override
    public void updatePostSelectedStatus(String[] input) throws ParseException {
        if(input.length < 2){
            System.out.println("参数错误");
            return;
        }
        Integer pageSize = Integer.valueOf(input[1]);
        long millis = 30000L;
        Integer execNums = 0;
        EsScrollResponse scrollResponse = EsFlushUtil.getInstance().searchByScroll(PostInfoIndexEntity.class, pageSize, null, millis);
        if (CollectionUtils.isEmpty(scrollResponse.getDatas())) {
            System.out.println("无可处理数据！");
            return;
        }
        List<PostInfoIndexEntity> postInfoIndexEntities  = scrollResponse.getDatas();
        setDefaultPostSelectedValue(postInfoIndexEntities);
        EsFlushUtil.getInstance().batchUpdate(postInfoIndexEntities);
        execNums = postInfoIndexEntities.size();
        String scrollId = scrollResponse.getScrollId();
        while (true){
            EsScrollResponse esScrollResponse = EsFlushUtil.getInstance().searchByScrollId(PostInfoIndexEntity.class, pageSize, scrollId, millis);
            if (esScrollResponse == null || CollectionUtils.isEmpty(esScrollResponse.getDatas())) {
                break;
            }
            setDefaultPostSelectedValue(esScrollResponse.getDatas());
            EsFlushUtil.getInstance().batchUpdate(esScrollResponse.getDatas());
            scrollId = esScrollResponse.getScrollId();
            execNums += esScrollResponse.getDatas().size();
            System.out.println("已处理帖子数 num=" + execNums);
        }
        System.out.println("帖子数据处理完成！");
    }

    private List<PostInfoIndexEntity> setDefaultPostSelectedValue(List<PostInfoIndexEntity> postInfoIndexEntityList) throws ParseException {
        for (PostInfoIndexEntity postInfoIndexEntity : postInfoIndexEntityList){
            //postInfoIndexEntity.setIsSelected(YesNoStatus.NO);
            postInfoIndexEntity.setTopTime(new Date());
        }
        return postInfoIndexEntityList;

    }

    @Override
    public void updateCommentIsAnonymousStatus(String[] input) {
        if(input.length < 2){
            System.out.println("参数错误");
            return;
        }
        Integer pageSize = Integer.valueOf(input[1]);
        long millis = 30000L;
        Integer execNums = 0;
        EsScrollResponse scrollResponse = EsFlushUtil.getInstance().searchByScroll(CommentInfoIndexEntity.class, pageSize, null, millis);
        if (CollectionUtils.isEmpty(scrollResponse.getDatas())) {
            System.out.println("无可处理数据！");
            return;
        }
        List<CommentInfoIndexEntity> commentInfoIndexEntities  = scrollResponse.getDatas();
        setDefaultCommentIsAnonymousValue(commentInfoIndexEntities);
        EsFlushUtil.getInstance().batchUpdate(commentInfoIndexEntities);
        execNums = commentInfoIndexEntities.size();
        String scrollId = scrollResponse.getScrollId();
        while (true){
            EsScrollResponse esScrollResponse = EsFlushUtil.getInstance().searchByScrollId(CommentInfoIndexEntity.class, pageSize, scrollId, millis);
            if (esScrollResponse == null || CollectionUtils.isEmpty(esScrollResponse.getDatas())) {
                break;
            }
            setDefaultCommentIsAnonymousValue(esScrollResponse.getDatas());
            EsFlushUtil.getInstance().batchUpdate(esScrollResponse.getDatas());
            scrollId = esScrollResponse.getScrollId();
            execNums += esScrollResponse.getDatas().size();
            System.out.println("已处理评论数 num=" + execNums);
        }
        System.out.println("评论数据处理完成！");
    }

    @Override
    public void updatePostCommunityId(String[] input) {
        if (input.length<2){
            System.out.println("参数错误");
            return;
        }
        String sourceCommunityId = input[0];
        String targetCommunityId = input[1];
        System.out.println("sourceCommunityId: "+sourceCommunityId+"\n targetCommunityId: "+targetCommunityId);
        List<PostInfo> postInfos = postInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QPostInfo.communityId,sourceCommunityId));
        postInfos.forEach(postInfo -> {
            postInfo.setCommunityId(targetCommunityId);
            postInfoApiService.update(postInfo);
            postInfoIndexApiService.updatePostCommunityId(postInfo.getPostId(),targetCommunityId);
        });

    }

    private List<CommentInfoIndexEntity> setDefaultCommentIsAnonymousValue(List<CommentInfoIndexEntity> commentInfoIndexEntities){
        for (CommentInfoIndexEntity commentInfoIndexEntity : commentInfoIndexEntities){
            commentInfoIndexEntity.setIsAnonymous(YesNoStatus.NO);
        }
        return commentInfoIndexEntities;
    }
}
