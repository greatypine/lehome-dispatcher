package cn.lehome.dispatcher.queue.listener.login;

import cn.lehome.base.api.tool.compoment.jms.EventBusComponent;
import cn.lehome.base.api.tool.constant.EventConstants;
import cn.lehome.base.api.tool.service.community.CommunityCacheApiService;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.token.SmartExchangeTokenService;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.event.StringEventMessage;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class LoginExchangeSmartTokenListener extends AbstractJobListener {

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private SmartExchangeTokenService smartExchangeTokenService;

    @Autowired
    private EventBusComponent eventBusComponent;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof StringEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        StringEventMessage stringEventMessage = (StringEventMessage) eventMessage;
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByOpenId(stringEventMessage.getData());
        if (userInfoIndex == null) {
            logger.error("用户索引信息未找到, userOpenId = {}", stringEventMessage.getData());
            return;
        }
        smartExchangeTokenService.exchangeAll(stringEventMessage.getData());
        eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.REFRESH_ENTRANCE_EVENT, userInfoIndex.getId()));
    }



    @Override
    public String getConsumerId() {
        return "login_exchange_smart_token";
    }
}
