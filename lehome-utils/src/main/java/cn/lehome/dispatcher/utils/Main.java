package cn.lehome.dispatcher.utils;

import cn.lehome.base.api.user.service.asset.UserBeanFlowApiService;
import cn.lehome.dispatcher.utils.activity.CalculateKeepActivityService;
import cn.lehome.dispatcher.utils.community.CommunityService;
import cn.lehome.dispatcher.utils.community.SmartDataService;
import cn.lehome.dispatcher.utils.content.ContentService;
import cn.lehome.dispatcher.utils.contribution.ContributionService;
import cn.lehome.dispatcher.utils.device.DeviceInfoSync;
import cn.lehome.dispatcher.utils.distribution.CalculateUserDistributionService;
import cn.lehome.dispatcher.utils.house.HouseService;
import cn.lehome.dispatcher.utils.merchant.GoodsService;
import cn.lehome.dispatcher.utils.operation.OperationService;
import cn.lehome.dispatcher.utils.push.PushService;
import cn.lehome.dispatcher.utils.robot.PostMaterielService;
import cn.lehome.dispatcher.utils.robot.RobotService;
import cn.lehome.dispatcher.utils.task.DailyConversionAccountService;
import cn.lehome.dispatcher.utils.user.UserMessageService;
import cn.lehome.dispatcher.utils.user.UserService;
import cn.lehome.dispatcher.utils.user.UserSync;
import cn.lehome.dispatcher.utils.user.UserTaskService;
import cn.lehome.framework.bean.core.enums.OperationType;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchDataAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;

import java.util.Date;
import java.util.List;
import java.util.Scanner;

import static java.lang.System.exit;

/**
 * 工具类调入口
 * Created by zuoguodong on 2018/3/20
 */
@SpringBootApplication(exclude = {org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration.class
        , org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration.class, JpaRepositoriesAutoConfiguration.class, ElasticsearchAutoConfiguration.class, ElasticsearchDataAutoConfiguration.class})
@ImportResource("classpath:META-INF/spring/*.xml")
@ComponentScan({"cn.lehome.dispatcher.utils", "cn.lehome.framework.configuration"})
public class Main implements CommandLineRunner {

    @Autowired
    private CommunityService communityService;

    @Autowired
    private HouseService houseService;

    @Autowired
    private UserService userService;

    @Autowired
    private UserTaskService userTaskService;

    @Autowired
    private RobotService robotService;

    @Autowired
    private PostMaterielService postMaterielService;

    @Autowired
    private ContentService contentService;

    @Autowired
    private DailyConversionAccountService dailyConversionAccountService;

    @Autowired
    private SmartDataService smartDataService;

    @Autowired
    private ContributionService contributionService;

    @Autowired
    private UserMessageService userMessageService;

    @Autowired
    private UserBeanFlowApiService userBeanFlowApiService;

    @Autowired
    private GoodsService goodsService;

    @Autowired
    private CalculateKeepActivityService calculateKeepActivityService;

    @Autowired
    private CalculateUserDistributionService calculateUserDistributionService;

    @Autowired
    private PushService pushService;

    @Autowired
    private OperationService operationService;

    @Autowired
    private DeviceInfoSync deviceInfoSync;

    @Autowired
    private UserSync userSync;

    public static void main(String args[]) {
        SpringApplication.run(Main.class, args);
    }

    private static void helpInfo() {
        System.out.println(
                "communityBindExt <excelFilePath> [sheetIndex]      --小区附加信息绑定\n" +
                        "updateCommunityExtName <id> <name>                 --修改小区名称\n" +
                        "repairHouseAddress <token> [id...]                 --补全房间地址\n" +
                        "refreshUserCache [pageNo]                          --刷新用户信息\n" +
                        "initUserAdditionalIndex [pageNo]                   --刷新用户附加索引信息\n" +
                        "refreshUserCacheByPhone [phone...]                 --通过手机号刷新用户信息\n" +
                        "repairUserCache                                    --修复用户缓存信息\n" +
                        "userTaskRecord                                     --刷新用户任务表\n" +
                        "inviteUserRecord                                   --刷新邀请记录表\n" +
                        "inviteUserChangeRecord                             --刷新邀请用户改变历史表\n" +
                        "drewStatics                                        --刷新用户领取奖金数据表\n" +
                        "initRobotHead <folderUrl>                          --初始化机器人头像\n" +
                        "changeRobotHead                                    --修改机器人头像\n" +
                        "initRobot <filePath> <sheetNum>                    --初始化机器人信息\n" +
                        "initPostMateriel <filePath> <sheetIndex> <typeId>  --初始化物料信息\n" +
                        "initCommunityPost [communityId...]                 --初始化小区贴子信息\n" +
                        "cleanCommunityPost                                 --清除机器人发的小区贴子信息\n" +
                        "initContentIndex <initCommentIndex|initPostIndex|initExtensionIndex|initLikesIndex>\n" +
                        "                 [pageNo]                          --初始化论坛索引信息\n" +
                        "deleteRepeatPost                                   --删除6公里内重复的贴子\n" +
                        "updatePostInfoIndexByMd5Key                        --通过MD5Key修改贴子索引信息\n" +
                        "kickUser <filePath>                                --用户数据清理\n" +
                        "coverExtensionToPost <filePath>  [sheetIndex]      --论坛数据上首页历史数据清理\n" +
                        "smartOpenDoor <startTime> <endTime> [communityExtId]     --重跑智社区开门数据\n" +
                        "repairExtensionRelationWithType  <filePath>  [sheetIndex]      " +
                        "                                           --修复历史广场话题数据\n" +
                        "userInformationStatistics                          --用户信息统计\n" +
                        "initScrollMessage                                  --初始化滚动消息\n" +
                        "dailyConversionAccountStatistics                   --金豆兑换财务统计\n" +
                        "contribution <date> <userId...>                    --徒弟进贡统计\n" +
                        "updateTaskOperationRecord                          --修改排除徒弟进贡和邀请好友任务的记录\n" +
                        "updateApprenticeOperationRecord                    --修改徒弟进贡和邀请好友任务的记录\n" +
                        "updateDailyInviteReward                            --修改账单流水每日邀请好友的记录\n" +
                        "updateUserMessageIndex                             --批量更新站内信推送记录表索引信息\n" +
                        "syncUserToHbase                                    --同步所有用户到Hbase中\n" +
                        "initMerchantIndex <[initGoodsIndex] [deleteGoodsInfoIndex]> [pageNo]        --初始化商品索引信息\n" +
                        "repairUserIndexWithDB [pageNo] <userId>            --刷新与数据库不一致的用户索引信息\n" +
                        "updateExt <access_token>                           --刷新智社区小区uniqueCode值\n" +
                        "calculateKeepActivityUser                          --用户留存统计\n" +
                        "calculateUserDistribution                          --用户分布统计\n" +
                        "pushFeedback                                       --创建推送回馈任务\n" +
                        "updateWeChatIndex [pageNo] <userId>                --刷新微信用户金豆索引数据\n" +
                        "createCancelTopPostTask                            --批量创建线上置顶帖子取消置顶任务\n" +
                        "updatePostSelectedStatus[pageSize]                 --刷新帖子精选默认值为0\n" +
                        "updateCommentIsAnonymousStatus[pageSize]           --批量刷新评论匿名字段为0\n" +
                        "updateUserRegisterSourceIndex [pageNo] <userId>    --刷新用户注册来源索引数据\n" +
                        "updateWeChatIndex [pageNo] <userId>              --刷新微信用户金豆索引数据\n" +
                        "flushIconData                                      --迁移icon旧数据\n" +
                        "migrateData<[migrateHomepageMouleData][<migrateFragmentData>][<migrateChannelData>]     --迁移homepageModule/channel/fragment数据\n" +
                        "updateAllCommunityExt                                      --刷新communityExt表缓存数据\n" +
                        "updateAllCommunity                                      --刷新Community表缓存数据\n" +
                        "updatePostCommunityId<sourceCommunityId><targetCommunityId>         --更新post表communityId\n" +
                        "updateEcommerceData                               --电商es数据迁移\n" +
                        "updateUserIndexFromExcel                                --从excel中刷新用户缓存\n" +
                        "refreshPrizeRedis [advertId]                           --刷新奖励金额\n" +
                        "syncDevice                                         --同步设备\n" +
                        "syncUser                                         --同步用户\n" +
                        "syncWechatUser                                         --同步微信用户\n" +
                        "exit                                               --退出\n" +
                        "help                                               --帮助\n"

        );
    }

    @Override
    public void run(String... strings) throws Exception {
        List<String> args = Lists.newArrayList();
        for (String str : strings) {
            System.out.println(str);
            if (str.equals("--spring.output.ansi.enabled=always")) {
                continue;
            }
            args.add(str);
        }


        if (args.size() > 0) {
            //后台守护运行
            execute(args.toArray(new String[args.size()]));
        } else {
            //交互式运行
            helpInfo();
            boolean exit = false;
            Scanner scanner = new Scanner(System.in);
            while (!exit) {
                String inputStr = scanner.nextLine();
                String input[] = inputStr.split(" ");
                if ("exit".equals(input[0])) {
                    exit = true;
                } else {
                    execute(input);
                }
            }
        }
        exit(0);
    }

    public void execute(String input[]) throws Exception {
        String command = input[0];
        switch (command) {
            case "help":
                helpInfo();
                break;
            case "communityBindExt":
                communityService.bindExt(input);
                break;
            case "updateCommunityExtName":
                communityService.updateCommunityExtName(input);
                break;
            case "repairHouseAddress":
                houseService.repairHouseAddress(input);
                break;
            case "refreshUserCache":
                userService.refreshUserCache(input);
                break;
            case "initUserAdditionalIndex":
                userService.initUserAdditionalIndex(input);
                break;

            case "repairUserIndexWithDB":
                userService.repairUserIndexWithDB(input);
                break;
            case "refreshUserCacheByPhone":
                userService.refreshUserCacheByPhone(input);
                break;
            case "repairUserCache":
                userService.repairUserCache();
                break;
            case "userTaskRecord":
                userTaskService.refreshUserTaskRecord();
                break;
            case "inviteUserRecord":
                userTaskService.refreshInviteUserRecord();
                break;
            case "inviteUserChangeRecord":
                userTaskService.refreshInviteUserChaneRecord();
                break;
            case "drewStatics":
                userTaskService.refreshDrewStatistcs();
                break;
            case "initRobotHead":
                robotService.initRobotHeadUrl(input[1]);
                break;
            case "changeRobotHead":
                robotService.changeRobotHead();
                break;
            case "initRobot":
                robotService.initRobot(input[1], Integer.valueOf(input[2]));
                break;
            case "initPostMateriel":
                postMaterielService.initPostMateriel(input);
                break;
            case "initCommunityPost":
                communityService.initPost(input);
                break;
            case "cleanCommunityPost":
                communityService.cleanPost();
                break;
            case "initContentIndex":
                contentService.initContentIndex(input);
                break;
            case "deleteRepeatPost":
                contentService.deleteRepeatPost();
                break;
            case "updatePostInfoIndexByMd5Key":
                contentService.updatePostInfoIndexByMd5Key(input);
                break;
            case "kickUser":
                userService.kickUser(input);
                break;
            case "coverExtensionToPost":
                contentService.coverExtensionToPost(input);
                break;
            case "repairExtensionRelationWithType":
                contentService.repairExtensionRelationWithType(input);
                break;
            case "userInformationStatistics":
                userService.userInformationStatistics();
                break;
            case "initScrollMessage":
                robotService.initScrollMessage();
                break;
            case "dailyConversionAccountStatistics":
                dailyConversionAccountService.dailyConversionAccountStatistics();
            case "smartOpenDoor":
                smartDataService.execOpenDoorData(input);
                break;
            case "contribution":
                contributionService.contribution(input);
                break;
            case "updateTaskOperationRecord":
                dailyConversionAccountService.updateTaskOperationRecord();
                break;
            case "updateApprenticeOperationRecord":
                dailyConversionAccountService.updateApprenticeOperationRecord();
                break;
            case "updateDailyInviteReward":
                dailyConversionAccountService.updateDailyInviteReward();
                break;
            case "updateUserMessageIndex":
                userMessageService.flushUserMessageIndexInfo();
                break;
            case "syncUserToHbase":
                userService.saveUserToHbase();
            case "testInvoke":
                Date start = new Date();
                Date end = new Date();
                start = DateUtils.setMilliseconds(DateUtils.setSeconds(DateUtils.setMinutes(DateUtils.setHours(start, 0), 0), 0), 0);
                end = DateUtils.setMilliseconds(DateUtils.setSeconds(DateUtils.setMinutes(DateUtils.setHours(end, 23), 59), 59), 999);
                userBeanFlowApiService.findSubBeanCountByUserId(10569L, Lists.newArrayList(OperationType.DEL_POST_MANAGER, OperationType.DEL_POST_SELF), start, end);
                break;
            case "initMerchantIndex":
                goodsService.initMerchantIndex(input);
                break;
            case "updateExt":
                String access_token = input[1];
                communityService.updateCommunityUniqueCode(access_token);
                break;
            case "calculateKeepActivityUser":
                calculateKeepActivityService.calculate();
                break;
            case "calculateUserDistribution":
                calculateUserDistributionService.calculate();
                break;
            case "pushFeedback":
                pushService.createScheduleJob();
            case "updateWeChatIndex":
                userService.updateWeChatIndex(input);
            case "createCancelTopPostTask":
                contentService.createCancelTopPostTask();
                break;
            case "updatePostSelectedStatus":
                contentService.updatePostSelectedStatus(input);
                break;
            case "updateCommentIsAnonymousStatus":
                contentService.updateCommentIsAnonymousStatus(input);
                break;
            case "updateUserRegisterSourceIndex":
                userService.updateUserRegisterSourceIndex(input);
                break;
            case "flushIconData":
                operationService.flushIconDate();
                break;
            case "migrateData":
                operationService.migrateData(input);
                break;
            case "updateAllCommunityExt":
                communityService.updateAllCommunityExt();
                break;
            case "updateAllCommunity":
                communityService.updateAllCommunity();
                break;
            case "updateUserIndexFromExcel":
                userService.updateUserIndexFromExcel(input);
                break;
            case "refreshPrizeRedis":
                dailyConversionAccountService.refreshPrizeRedis(input);
                break;
            case "syncDevice":
                Long startId = null;
                if (input.length == 2) {
                    startId = Long.valueOf(input[1]);
                }
                deviceInfoSync.sync(startId);
                break;
            case "syncUser":
                startId = null;
                if (input.length == 2) {
                    startId = Long.valueOf(input[1]);
                }
                userSync.sync(startId);
                break;
            case "syncWechatUser":
                userSync.wechatSync();
                break;
            default:
                if (!"".equals(command)) {
                    System.out.println("command error");
                }
        }
    }
}
