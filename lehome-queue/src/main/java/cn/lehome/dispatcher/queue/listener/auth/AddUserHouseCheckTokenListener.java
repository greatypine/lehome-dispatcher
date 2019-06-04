package cn.lehome.dispatcher.queue.listener.auth;

import cn.lehome.base.api.common.bean.community.CommunityExt;
import cn.lehome.base.api.common.component.jms.EventBusComponent;
import cn.lehome.base.api.common.constant.EventConstants;
import cn.lehome.base.api.common.event.AddUserHouseEventBean;
import cn.lehome.base.api.common.service.community.CommunityCacheApiService;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.common.enums.community.EditionType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.dispatcher.queue.service.token.SmartExchangeTokenService;
import cn.lehome.framework.base.api.core.bean.SmartTokenBean;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.exception.sql.NotFoundRecordException;
import cn.lehome.framework.base.api.core.service.AuthorizationService;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class AddUserHouseCheckTokenListener extends AbstractJobListener {

    @Autowired
    private UserHouseRelationshipApiService userHouseRelationshipApiService;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private EventBusComponent eventBusComponent;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Autowired
    private SmartExchangeTokenService smartExchangeTokenService;

    @Value("${property.new.flag}")
    private Boolean isNewFlag;



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof SimpleEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<AddUserHouseEventBean> simpleEventMessage = (SimpleEventMessage) eventMessage;
        AddUserHouseEventBean addUserHouseEventBean = simpleEventMessage.getData();
        UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.get(addUserHouseEventBean.getObjectId());
        if (userHouseRelationship == null) {
            throw new NotFoundRecordException(String.format("用户房产信息未找到, relationId = %s", addUserHouseEventBean.getObjectId()));
        }

        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userHouseRelationship.getUserId());
        if (userInfoIndex == null) {
            throw new NotFoundRecordException(String.format("用户索引未找到, userId = %s", userHouseRelationship.getUserId()));
        }

        CommunityExt communityExt = communityCacheApiService.getCommunityExt(userHouseRelationship.getCommunityExtId());
        if (communityExt.getEditionType().equals(EditionType.pro) && StringUtils.isNotEmpty(communityExt.getUniqueCode())) {
            SmartTokenBean smartTokenBean = authorizationService.getSmartToken(userInfoIndex.getUserOpenId(), userHouseRelationship.getCommunityExtId());
            if (smartTokenBean == null) {
                if (isNewFlag) {
                    smartExchangeTokenService.exchangeSmartToken(userInfoIndex.getUserOpenId(), communityExt.getUniqueCode());
                } else {
                    smartExchangeTokenService.oldExchangeSmartToken(userInfoIndex.getUserOpenId(), communityExt);
                }

            }
        }
        if (userInfoIndex.getIsLogin().equals(YesNoStatus.YES)) {
            eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.REFRESH_ENTRANCE_EVENT, userHouseRelationship.getUserId()));
        }
    }



    @Override
    public String getConsumerId() {
        return "add_user_house_check_token";
    }
}
