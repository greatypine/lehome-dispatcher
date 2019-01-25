package cn.lehome.dispatcher.utils.robot;

/**
 * Created by wuzhao on 2018/4/2.
 */
public interface RobotService {

    void initRobotHeadUrl(String folderUrl);

    void initRobot(String filePath, Integer sheetNum);

    void changeRobotHead();

    void initScrollMessage();
}
