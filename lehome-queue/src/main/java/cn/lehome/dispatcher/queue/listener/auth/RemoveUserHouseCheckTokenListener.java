package cn.lehome.dispatcher.queue.listener.auth;

import cn.lehome.base.api.tool.bean.community.CommunityExt;
import cn.lehome.base.api.tool.compoment.jms.EventBusComponent;
import cn.lehome.base.api.tool.constant.EventConstants;
import cn.lehome.base.api.tool.event.RemoveUserHouseEventBean;
import cn.lehome.base.api.tool.service.community.CommunityCacheApiService;
import cn.lehome.base.api.user.bean.user.UserHouseRelationship;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserHouseRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.tool.entity.enums.community.EditionType;
import cn.lehome.bean.user.entity.enums.user.HouseType;
import cn.lehome.dispatcher.queue.listener.AbstractJobListener;
import cn.lehome.framework.base.api.core.event.IEventMessage;
import cn.lehome.framework.base.api.core.event.LongEventMessage;
import cn.lehome.framework.base.api.core.event.SimpleEventMessage;
import cn.lehome.framework.base.api.core.exception.sql.NotFoundRecordException;
import cn.lehome.framework.base.api.core.service.AuthorizationService;
import cn.lehome.framework.bean.core.enums.EnableDisableStatus;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2018/3/13.
 */
public class RemoveUserHouseCheckTokenListener extends AbstractJobListener {

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



    @Override
    public void execute(IEventMessage eventMessage) throws Exception {
        if (!(eventMessage instanceof SimpleEventMessage)) {
            logger.error("消息类型不对");
            return;
        }
        SimpleEventMessage<RemoveUserHouseEventBean> simpleEventMessage = (SimpleEventMessage) eventMessage;
        RemoveUserHouseEventBean removeUserHouseEventBean = simpleEventMessage.getData();
        UserHouseRelationship userHouseRelationship = userHouseRelationshipApiService.get(removeUserHouseEventBean.getObjectId());
        if (userHouseRelationship == null) {
            throw new NotFoundRecordException(String.format("用户房产信息未找到, relationId = %s", removeUserHouseEventBean.getObjectId()));
        }
        CommunityExt communityExt = communityCacheApiService.getCommunityExt(userHouseRelationship.getCommunityExtId());
        if (communityExt.getEditionType().equals(EditionType.pro)) {
            List<UserHouseRelationship> userHouseRelationshipList = userHouseRelationshipApiService.findByUserId(userHouseRelationship.getUserId());
            Set<Long> authCommunityIds = Sets.newHashSet();
            authCommunityIds.addAll(userHouseRelationshipList.stream().filter(relationship -> !relationship.getHouseType().equals(HouseType.FORBID) && relationship.getEnableStatus().equals(EnableDisableStatus.ENABLE)).map(UserHouseRelationship::getCommunityExtId).collect(Collectors.toList()));
            if (!authCommunityIds.contains(userHouseRelationship.getCommunityExtId())) {
                UserInfoIndex userInfoIndex = userInfoIndexApiService.findByUserId(userHouseRelationship.getUserId());
                if (userInfoIndex == null) {
                    throw new NotFoundRecordException(String.format("用户索引未找到, userId = %s", userHouseRelationship.getUserId()));
                }
                authorizationService.removeSmartToken(userInfoIndex.getUserOpenId(), userHouseRelationship.getCommunityExtId());
            }
        }
        eventBusComponent.sendEventMessage(new LongEventMessage(EventConstants.REFRESH_ENTRANCE_EVENT, userHouseRelationship.getUserId()));
    }



    @Override
    public String getConsumerId() {
        return "remove_user_house_check_token";
    }
}
