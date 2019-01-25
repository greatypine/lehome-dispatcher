package cn.lehome.dispatcher.utils.robot;

import cn.lehome.base.api.advertising.service.task.TaskSettingApiService;
import cn.lehome.base.api.content.bean.robot.Robot;
import cn.lehome.base.api.content.service.robot.RobotApiService;
import cn.lehome.base.api.tool.bean.storage.StorageInfo;
import cn.lehome.base.api.tool.compoment.storage.AliyunOSSComponent;
import cn.lehome.base.api.user.bean.user.QUserInfo;
import cn.lehome.base.api.user.bean.user.UserInfo;
import cn.lehome.base.api.user.service.user.UserInfoApiService;
import cn.lehome.bean.content.entity.enums.robot.RobotType;
import cn.lehome.bean.tool.entity.enums.storage.StorageObjectType;
import cn.lehome.bean.tool.entity.enums.storage.StorageUsageType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.ExcelUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Created by wuzhao on 2018/4/2.
 */
@Service
public class RobotServiceImpl implements RobotService  {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private AliyunOSSComponent aliyunOSSComponent;

    @Autowired
    private RobotApiService robotApiService;

    @Autowired
    private UserInfoApiService userInfoApiService;

    @Autowired
    private TaskSettingApiService taskSettingApiService;

    private static final String ROBOT_HEAD_KEY = "robot_head";

    private static final String ROBOT_MSG_KEY = "robot_scroll_message_key";

    @Override
    public void initRobotHeadUrl(String folderUrl) {
        ListOperations listOperations = stringRedisTemplate.opsForList();
        File folder = new File(folderUrl);
        if (!folder.isDirectory()) {
            System.out.println(String.format("文件夹地址不是一个合法的文件夹"));
            return;
        }
        for (File file : folder.listFiles()) {
            if (file.isFile()) {
                try {
                    InputStream inputStream = new FileInputStream(file);
                    StorageInfo storageInfo = aliyunOSSComponent.putObject(inputStream, "ROBOT" + UUID.randomUUID() + ".png", null, StorageObjectType.USER, StorageUsageType.USER_AVATAR);
                    String headUrl = String.format("%s%s", storageInfo.getPrefix(), storageInfo.getRelativeUrl());
                    listOperations.leftPush(ROBOT_HEAD_KEY, headUrl);
                } catch (FileNotFoundException e) {
                    System.out.println("创建文件流失败, filePath = " + file.getAbsoluteFile());
                }
            }
        }
    }

    @Override
    public void initRobot(String filePath, Integer sheetNum) {
        String file = filePath;
        int sheetIndex = sheetNum;
        try {
            ListOperations<String, String> listOperations = stringRedisTemplate.opsForList();
            ExcelUtils excelUtils = new ExcelUtils(file);
            List<List<String>> data = excelUtils.read(sheetIndex);
            int i = 0;
            for (List<String> rowList : data) {
                if (rowList.size() < 1) {
                    continue;
                }
                String nickName = rowList.get(0).trim();
                Robot robot = new Robot();
                ApiResponse<UserInfo> apiResponse = userInfoApiService.findAll(ApiRequest.newInstance().filterEqual(QUserInfo.nickName, nickName), ApiRequestPage.newInstance().paging(0, 1));
                if (apiResponse.getTotal() > 0l) {
                    Random random = new Random();
                    nickName = nickName + random.nextInt(10);
                }
                robot.setNickName(nickName);

                String headUrl = listOperations.rightPopAndLeftPush(ROBOT_HEAD_KEY, ROBOT_HEAD_KEY);
                robot.setHeadUrl(headUrl);
                robot.setRobotType(RobotType.NOT_OFFICIAL);
                robotApiService.save(robot);
                i++;
                if (i % 10 == 0) {
                    System.out.print(".");
                }
                if (i % 100 == 0) {
                    System.out.println();
                }
            }
            System.out.println("finished");

        } catch(Exception e){
            System.out.println("数据处理出错：" + e.getMessage());
            e.printStackTrace();
            return;
        }

    }

    @Override
    public void changeRobotHead() {
        int pageSize = 100;
        int pageIndex = 0;
        ListOperations<String, String> listOperations = stringRedisTemplate.opsForList();
        ApiResponse<Robot> apiResponse = robotApiService.findAll(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(pageIndex, pageSize));
        while(apiResponse.getCount()>0){
            apiResponse.getPagedData().forEach(robot -> {
                String headUrl = listOperations.rightPopAndLeftPush(ROBOT_HEAD_KEY, ROBOT_HEAD_KEY);
                robot.setHeadUrl(headUrl);
                robotApiService.save(robot);
                System.out.print(".");
            });
            pageIndex ++;
            apiResponse = robotApiService.findAll(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(pageIndex, pageSize));
            System.out.println();
        }
        System.out.println("数据处理完毕");
    }

    @Override
    public void initScrollMessage() {
        ApiResponse<Robot> apiResponse = robotApiService.findAll(ApiRequest.newInstance(), ApiRequestPage.newInstance().paging(0, 10));
        ListOperations<String, String> listOperations = stringRedisTemplate.opsForList();

        apiResponse.getPagedData().forEach(robot -> {
            int money = (new Random().nextInt())+1;
            listOperations.leftPush(ROBOT_MSG_KEY,robot.getNickName() + "邀请了好友，获得"+(new Random().nextInt(20)+1)+"元现金");
        });
        System.out.println("数据处理完毕");
    }

}
