package cn.lehome.dispatcher.utils.community;

import java.text.ParseException;

public interface SmartDataService {
    /**
     * 重跑指定时间范围内智社区开门数据
     * @param args [1] 开始时间 [2] 结束时间 [3] 指定小区
     * 开始时间 结束时间均不加年月日
     */
    public void execOpenDoorData(String[] args) throws ParseException;

}
