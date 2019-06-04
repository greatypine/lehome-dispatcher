package cn.lehome.dispatcher.queue.service.impl.token;

import cn.lehome.base.api.common.bean.community.CommunityExt;
import cn.lehome.base.api.common.service.community.CommunityCacheApiService;
import cn.lehome.base.api.property.bean.oauth.SmartTokenInfo;
import cn.lehome.base.api.property.bean.oauth.TokenInfo;
import cn.lehome.base.api.property.service.oauth.OauthInfoApiService;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;
import cn.lehome.base.api.user.service.user.UserInfoIndexApiService;
import cn.lehome.bean.common.enums.community.EditionType;
import cn.lehome.dispatcher.queue.service.impl.AbstractBaseServiceImpl;
import cn.lehome.dispatcher.queue.service.token.SmartExchangeTokenService;
import cn.lehome.framework.base.api.core.bean.SmartTokenBean;
import cn.lehome.framework.base.api.core.bean.SmartUserTokenBean;
import cn.lehome.framework.base.api.core.exception.BaseApiException;
import cn.lehome.framework.base.api.core.exception.sql.NotFoundRecordException;
import cn.lehome.framework.base.api.core.service.AuthorizationService;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by wuzhao on 2018/3/19.
 */
@Service
public class SmartExchangeTokenServiceImpl extends AbstractBaseServiceImpl implements SmartExchangeTokenService {

    @Autowired
    private OauthInfoApiService oauthInfoApiService;

    @Autowired
    private AuthorizationService authorizationService;

    @Autowired
    private UserInfoIndexApiService userInfoIndexApiService;

    @Autowired
    private CommunityCacheApiService communityCacheApiService;

    @Value("${property.new.flag}")
    private Boolean isNewFlag;

    @Override
    public void exchangeAll(String userOpenId) {
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByOpenId(userOpenId);
        if (userInfoIndex == null) {
            logger.error("用户索引信息未找到, userOpenId = {}", userOpenId);
            return;
        }

        if (CollectionUtils.isEmpty(userInfoIndex.getAuthCommunityIds())) {
            logger.error("没有认证的小区, 不需要换取token");
            return;
        }
        Map<Long, CommunityExt> communityExtMap = communityCacheApiService.findAllCommunityExt(userInfoIndex.getAuthCommunityIds());
        List<CommunityExt> needExchangeTokenList = communityExtMap.values().stream().filter(communityExt -> communityExt.getEditionType().equals(EditionType.pro)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(needExchangeTokenList)) {
            logger.error("认证的小区中不包含pro小区, 不需要换取token");
            return;
        }
        if (isNewFlag) {
            Set<String> uniqueCodeSet = Sets.newHashSet();
            for (CommunityExt communityExt : needExchangeTokenList) {
                logger.error("需要换取token的小区ID : " + communityExt.getPropertyCommunityId() + ",uniqueCode : " + communityExt.getUniqueCode());
                if (StringUtils.isNotEmpty(communityExt.getUniqueCode())) {
                    uniqueCodeSet.add(communityExt.getUniqueCode());
                }
            }

            if (!CollectionUtils.isEmpty(uniqueCodeSet)) {

                for (String uniqueCode : uniqueCodeSet) {
                    try {
                        exchangeSmartToken(userOpenId, uniqueCode);
                    } catch (BaseApiException e) {
                        logger.error("获取smart_token失败:", e);
                    }
                }
            }
        } else {
            for (CommunityExt communityExt : needExchangeTokenList) {
                try {
                    this.oldExchangeSmartToken(userOpenId, communityExt);
                } catch (Exception e) {
                    logger.error("换取token失败, communityId = {}, userOpenId = {}", communityExt.getId(), userOpenId, e);
                }
            }
        }
    }

    @Override
    public void refreshAll(String userOpenId) {
        logger.error("刷新智社区token, userOpenId = {}", userOpenId);
        UserInfoIndex userInfoIndex = userInfoIndexApiService.findByOpenId(userOpenId);
        if (userInfoIndex == null) {
            logger.error("用户索引信息未找到, userOpenId = {}", userOpenId);
            return;
        }

        if (CollectionUtils.isEmpty(userInfoIndex.getAuthCommunityIds())) {
            logger.error("没有认证的小区, 不需要换取token");
            return;
        }
        Map<Long, CommunityExt> communityExtMap = communityCacheApiService.findAllCommunityExt(userInfoIndex.getAuthCommunityIds());
        List<CommunityExt> needExchangeTokenList = communityExtMap.values().stream().filter(communityExt -> communityExt.getEditionType().equals(EditionType.pro)).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(needExchangeTokenList)) {
            logger.error("认证的小区中不包含pro小区, 不需要换取token");
        }
        if (isNewFlag) {
            Set<String> uniqueCodeSet = Sets.newHashSet();
            for (CommunityExt communityExt : needExchangeTokenList) {
                if (communityExt.getEditionType().equals(EditionType.pro) && StringUtils.isNotEmpty(communityExt.getUniqueCode())) {
                    uniqueCodeSet.add(communityExt.getUniqueCode());
                }
            }

            if (!CollectionUtils.isEmpty(uniqueCodeSet)) {
                for (String uniqueCode : uniqueCodeSet) {
                    try {
                        exchangeSmartToken(userOpenId, uniqueCode);
                    } catch (BaseApiException e) {
                        logger.error("获取smart_token失败:", e);
                    }
                }
            }
        } else {
            for (CommunityExt communityExt : needExchangeTokenList) {
                logger.error("检查token, userOpenId = {}, communityExtId = {}", userOpenId, communityExt.getId());
                SmartTokenBean smartTokenBean = authorizationService.getSmartToken(userOpenId, communityExt.getId());
                if (smartTokenBean == null) {
                    logger.error("userOpenId = {}, communityExtId = {} 没有token换取token", userOpenId, communityExt.getId());
                    this.oldExchangeSmartToken(userOpenId, communityExt);
                } else {
                    if (authorizationService.checkNeedRefreshToken(userOpenId, communityExt.getId())) {
                        logger.error("userOpenId = {}, communityExtId = {} 刷新token", userOpenId, communityExt.getId());
                        try {
                            TokenInfo tokenInfo = oauthInfoApiService.refreshToken(smartTokenBean.getRefreshToken(), communityExt.getPropertyCommunityId());
                            authorizationService.storeSmartToken(userOpenId, tokenInfo.getAccess_token(), communityExt.getId(), tokenInfo.getRefresh_token(), tokenInfo.getExpires_in());
                        } catch (Exception e) {
                            logger.error("刷新token失败 : ", e);
                            logger.error("刷新失败, 从新获取token");
                            this.getSmartToken(userOpenId, communityExt);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void exchangeSmartToken(String userOpenId, String uniqueCode) {
        String apiToken = authorizationService.getApiToken(userOpenId);
        if (StringUtils.isEmpty(apiToken)) {
            throw new NotFoundRecordException(String.format("用户apiToken未找到, userOpenId = %s", userOpenId));
        }
        SmartUserTokenBean smartUserTokenBean = authorizationService.getSmartUserToken(userOpenId, uniqueCode);
        if (smartUserTokenBean != null) {
            logger.error("smart_token未失效, 不进行获取token操作");
            authorizationService.storeSmartUserTokenKey(userOpenId, uniqueCode);
            return;
        }
        SmartTokenInfo smartTokenInfo = oauthInfoApiService.getAllToken(authorizationService.getApiToken(userOpenId), uniqueCode);
        smartUserTokenBean = new SmartUserTokenBean();
        smartUserTokenBean.setAccessToken(smartTokenInfo.getAccess_token());
        smartUserTokenBean.setUniqueCode(uniqueCode);
        smartUserTokenBean.setExpireTime(smartTokenInfo.getExpires_in());
        smartUserTokenBean.setUserToken(smartTokenInfo.getUser_token());
        authorizationService.storeSmartUserToken(userOpenId, smartUserTokenBean);
    }

    @Override
    public void oldExchangeSmartToken(String userOpenId, CommunityExt communityExt) {
        String apiToken = authorizationService.getApiToken(userOpenId);
        if (StringUtils.isEmpty(apiToken)) {
            throw new NotFoundRecordException(String.format("用户apiToken未找到, userOpenId = %s", userOpenId));
        }
        SmartTokenBean smartTokenBean = authorizationService.getSmartToken(userOpenId, communityExt.getId());
        if (smartTokenBean == null) {
            try {
                TokenInfo tokenInfo = oauthInfoApiService.getToken(apiToken, communityExt.getPropertyCommunityId());
                authorizationService.storeSmartToken(userOpenId, tokenInfo.getAccess_token(), communityExt.getId(), tokenInfo.getRefresh_token(), tokenInfo.getExpires_in());
            } catch (Exception e) {
                logger.error("换取token失败 : ", e);
            }
        } else {
            logger.error("已经存在物管token, 不需要跟换");
        }
    }

    private void getSmartToken(String userOpenId, CommunityExt communityExt) {
        try {
            String apiToken = authorizationService.getApiToken(userOpenId);
            TokenInfo tokenInfo = oauthInfoApiService.getToken(apiToken, communityExt.getPropertyCommunityId());
            authorizationService.storeSmartToken(userOpenId, tokenInfo.getAccess_token(), communityExt.getId(), tokenInfo.getRefresh_token(), tokenInfo.getExpires_in());
        } catch (Exception e) {
            logger.error("换取token失败 : ", e);
        }
    }

}
