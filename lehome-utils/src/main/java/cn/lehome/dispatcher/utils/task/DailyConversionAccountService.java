package cn.lehome.dispatcher.utils.task;

/**
 * @author yanwenkai
 * @date 2018/5/23
 */
public interface DailyConversionAccountService {
    void updateDailyInviteReward();

    void updateApprenticeOperationRecord();

    void updateTaskOperationRecord();

    void dailyConversionAccountStatistics();

    void refreshPrizeRedis(String[] input);
}
