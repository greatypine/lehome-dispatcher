package cn.lehome.dispatcher.queue.listener.login;

import cn.lehome.base.api.advertising.constant.JoinActivityTypeConstants;
import cn.lehome.base.api.property.bean.households.AuthHouseholdsInfo;
import cn.lehome.base.api.property.service.households.HouseholdsInfoApiService;
import cn.lehome.base.api.tool.bean.community.Community;
import cn.lehome.base.api.tool.bean.community.CommunityExt;
import cn.lehome.base.api.tool.compoment.jms.EventBusComponent;
import cn.lehome.base.api.tool.constant.EventConstants;
import cn.lehome.base.api.tool.event.JoinActivityEventBean;
import cn.lehome.base.api.tool.service.community.CommunityApiService;
import cn.lehome.base.api.tool.service.community.CommunityCacheApiService;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.tool.entity.enums.community.EditionType;
import cn.lehome.bean.user.entity.enums.user.HouseType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.house.HouseholdService;
import cn.lehome.framework.base.api.core.constant.HeaderKeyContants;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.event.StringEventMessage;
import cn.lehome.framework.base.api.core.service.AuthorizationService;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

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
