package cn.lehome.dispatcher.queue.listener.park;

import cn.lehome.base.api.park.bean.park.ParkMessage;
import cn.lehome.base.api.park.bean.parkcar.ParkCarRegistered;
import cn.lehome.base.api.park.service.park.ParkMessageApiService;
import cn.lehome.base.api.park.service.parkcar.ParkCarRegisteredApiService;
import cn.lehome.bean.park.enums.parkcar.ParkCarProperties;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author: sunwj@sqbj.com
 * 获取创建车场消息后，进行批量创建车位的任务
 * @Date: 2019/12/5 3:30 下午
 */
public class MakeParkCarRegisterListener extends AbstractJobListener {

    @Autowired
    private ParkCarRegisteredApiService parkCarRegisteredApiService;

    @Autowired
    private ParkMessageApiService parkMessageApiService;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof LongEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        LongEventMessage longEventMessage = (LongEventMessage) eventMessage;

        ParkMessage parkMessage =parkMessageApiService.findById(longEventMessage.getData().intValue());
        String regionDetail = parkMessage.getRegionDetail();
        int parkId = parkMessage.getId();
        Date today = new Date();
        Map<String, String> regionMap = JSONObject.parseObject(regionDetail, Map.class);

        Iterator<Map.Entry<String, String>> iterator = regionMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            int carRegisteredNum = Integer.valueOf(entry.getValue());
            String carRegisteredName = entry.getKey();
            for (int i = 1 ; i < carRegisteredNum+1 ; i++) {
                ParkCarRegistered parkCarRegistered = new ParkCarRegistered();
                parkCarRegistered.setParkId(parkId);
                parkCarRegistered.setParkRegion(carRegisteredName);
                String numStr=String.format(carRegisteredName+"%03d",i);
                parkCarRegistered.setParkcarNo(numStr);
                System.out.println(parkCarRegistered.getParkcarNo());
                parkCarRegistered.setCarNum("");
                parkCarRegistered.setParkcarOwner("");
                parkCarRegistered.setOwnerAddress("");
                parkCarRegistered.setOwnerPhone("");
                parkCarRegistered.setParkcarStartTime(today);
                parkCarRegistered.setParkcarEndTime(today);
                parkCarRegistered.setParkcarProperties(ParkCarProperties.AGENT);
                parkCarRegisteredApiService.create(parkCarRegistered);
            }
        }
    }

    @Override
    public String getConsumerId() {
        return "ParkCar_Registered_Creat_Done";
    }
}
