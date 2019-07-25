package cn.lehome.dispatcher.queue.listener.workorder;

import cn.lehome.base.api.workorder.bean.manager.AdminScoreStatistic;
import cn.lehome.base.api.workorder.bean.manager.UserScore;
import cn.lehome.base.api.workorder.service.manager.AdminManagerApiService;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;

/**
 * Created by wuzhao on 2019/5/31.
 */
public class ScoreStatisticListener extends AbstractJobListener {

    @Autowired
    private AdminManagerApiService adminManagerApiService;

    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;
        UserScore userScore = adminManagerApiService.getUserScore(longEventMessage.getData());
        if (userScore == null) {
            logger.error("评分信息未找到, id = " + longEventMessage.getData());
            return;
        }
        AdminScoreStatistic adminScoreStatistic = adminManagerApiService.findStatisticByAdminId(userScore.getAdminId(), userScore.getYear(), userScore.getMonth());
        if (adminScoreStatistic == null) {
            adminScoreStatistic = new AdminScoreStatistic();
            adminScoreStatistic.setAdminId(userScore.getAdminId());
            adminScoreStatistic.setMonth(userScore.getMonth());
            adminScoreStatistic.setYear(userScore.getYear());
            adminScoreStatistic.setScore(new BigDecimal(userScore.getScore()));
            adminScoreStatistic.setScoreUserCount(1);
        } else {
            BigDecimal score = adminScoreStatistic.getScore().multiply(new BigDecimal(adminScoreStatistic.getScoreUserCount())).add(new BigDecimal(userScore.getScore())).divide(new BigDecimal(adminScoreStatistic.getScoreUserCount() + 1)).setScale(0, BigDecimal.ROUND_UP);
            adminScoreStatistic.setScore(score);
            adminScoreStatistic.setScoreUserCount(adminScoreStatistic.getScoreUserCount() + 1);
        }
        adminManagerApiService.createStatistic(adminScoreStatistic);
    }

    @Override
    public String getConsumerId() {
        return "score_statistic";
    }

}
