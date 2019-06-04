package cn.lehome.dispatcher.queue.listener.login;

import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.house.HouseholdService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.StringEventMessage;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class FirstLoginAuthListener extends AbstractJobListener {

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private HouseholdService householdService;


    @Override
    public void execute(IEventMessage eventMessage){
        if (!(eventMessage instanceof StringEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        StringEventMessage stringEventMessage = (StringEventMessage) eventMessage;
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByOpenId(stringEventMessage.getData());
        if (userInfoIndex == null) {
            logger.error("用户信息未找到, userOpenId = {}", stringEventMessage.getData());
            return;
        }

        householdService.syncHouseholdInfo(userInfoIndex);

    }

    @Override
    public String getConsumerId() {
        return "first_login_auth";
    }
}
