package cn.lehome.dispatcher.utils.user;

import cn.lehome.base.api.oauth2.bean.device.MobileDevice;
import cn.lehome.base.api.oauth2.bean.user.*;
import cn.lehome.base.api.oauth2.service.device.ClientDeviceApiService;
import cn.lehome.base.api.oauth2.service.user.UserAccountApiService;
import cn.lehome.base.api.tool.bean.device.ClientDevice;
import cn.lehome.base.api.tool.service.device.DeviceApiService;
import cn.lehome.base.api.user.bean.user.QUserInfo;
import cn.lehome.base.api.user.bean.user.UserDeviceRelationship;
import cn.lehome.base.api.user.bean.user.UserInfo;
import cn.lehome.base.api.user.service.user.UserDeviceRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoApiService;
import cn.lehome.bean.oauth2.enums.user.SexType;
import cn.lehome.bean.user.entity.enums.wechat.UserRegisterType;
import cn.lehome.base.api.user.bean.wechat.QUserWeChatRelation;
import cn.lehome.base.api.user.bean.wechat.QWeChatInfo;
import cn.lehome.base.api.user.bean.wechat.UserWeChatRelation;
import cn.lehome.base.api.user.bean.wechat.WeChatInfo;
import cn.lehome.base.api.user.service.user.UserDeviceRelationshipApiService;
import cn.lehome.base.api.user.service.user.UserInfoApiService;
import cn.lehome.base.api.user.service.wechat.UserWeChatRelationApiService;
import cn.lehome.base.api.user.service.wechat.WeChatInfoApiService;
import cn.lehome.bean.oauth2.enums.user.AnonymousType;
import cn.lehome.bean.oauth2.enums.user.SexType;
import cn.lehome.bean.user.entity.enums.wechat.UserRegisterType;
import cn.lehome.bean.user.entity.enums.wechat.WeChatOpenIdType;
import cn.lehome.framework.base.api.core.enums.PageOrderType;
import cn.lehome.framework.base.api.core.request.ApiRequest;
import cn.lehome.framework.base.api.core.request.ApiRequestPage;
import cn.lehome.framework.base.api.core.response.ApiResponse;
import cn.lehome.framework.base.api.core.util.CoreStringUtils;
import cn.lehome.framework.base.api.core.util.MD5Util;
import cn.lehome.framework.bean.core.enums.ClientOSType;
import cn.lehome.framework.bean.core.enums.ClientType;
import cn.lehome.framework.bean.core.enums.DeviceType;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Created by wuzhao on 2018/12/4.
 */
@Service("userSync")
public class UserSyncImpl implements UserSync {

    @Autowired
    private UserAccountApiService userAccountApiService;

    @Autowired
    private ClientDeviceApiService clientDeviceApiService;

    @Autowired
    private UserInfoApiService userInfoApiService;

    @Autowired
    private UserDeviceRelationshipApiService userDeviceRelationshipApiService;

    @Autowired
    private WeChatInfoApiService weChatInfoApiService;

    @Autowired
    private UserWeChatRelationApiService userWeChatRelationApiService;

    @Autowired
    private DeviceApiService deviceApiService;

    private static Logger logger = LoggerFactory.getLogger(UserSync.class);

    private static final String USER_KEY_PREFIX = "BJ_";

    @Override
    public void sync(Long startId) {
        ApiRequestPage requestPage = ApiRequestPage.newInstance().paging(0, 100).addOrder(QUserInfo.userId, PageOrderType.ASC);
        ApiRequest apiRequest = ApiRequest.newInstance();
        if (startId != null && startId > 0L) {
            apiRequest.filterGreaterEqual(QUserInfo.userId, startId);
        }

        int i = 0;
        int j = 0;
        int k = 0;
        List<Long> failedList = Lists.newArrayList();
        while (true) {
            ApiResponse<UserInfo> apiResponse = userInfoApiService.findAll(apiRequest, requestPage);

            if (apiResponse == null || apiResponse.getCount() == 0) {
                break;
            }

            for (UserInfo userInfo : apiResponse.getPagedData()) {
                if (!CoreStringUtils.checkChinaMobilePhone(userInfo.getPhone())) {
                    j++;
                    continue;
                }
                int retryTimes = 0;
                while (retryTimes < 3) {
                    try {
                        syncUserInfo(userInfo);
                        break;
                    } catch (Exception e) {
                        retryTimes++;
                        if (retryTimes == 3) {
                            k++;
                            failedList.add(userInfo.getUserId());
                            logger.error("用户信息同步失败:" + userInfo.getUserId(), e);
                        }
                    }
                }
            }

            i += apiResponse.getPagedData().size();
            System.out.println("同步完成" + i + "条数据" + " 手机号不正确忽略" + j + "条数据" + " 同步失败" + k + "条数据");
            if (failedList.size() > 0) {
                System.out.println("出错用户ID : " + StringUtils.join(failedList, ","));
            }


            if (apiResponse.getCount() < requestPage.getPageSize()) {
                break;
            }

            requestPage.pagingNext();
        }
        System.out.println("全部同步完成, 总共 ： " + i);
    }

    @Override
    public void wechatSync(String wxUnionId) {

        ApiRequestPage requestPage = ApiRequestPage.newInstance().paging(0, 100).addOrder(QWeChatInfo.createdTime, PageOrderType.ASC);
        ApiRequest apiRequest = ApiRequest.newInstance();
        if (StringUtils.isNotEmpty(wxUnionId)) {
            apiRequest.filterEqual(QWeChatInfo.wxUnionId, wxUnionId);
        }

        int i = 0;
        int j = 0;
        int k = 0;
        List<String> failedList = Lists.newArrayList();
        while (true) {
            ApiResponse<WeChatInfo> apiResponse = weChatInfoApiService.findAll(apiRequest, requestPage);

            if (apiResponse == null || apiResponse.getCount() == 0) {
                break;
            }

            for (WeChatInfo weChatInfo : apiResponse.getPagedData()) {
                int retryTimes = 0;
                while (retryTimes < 3) {
                    try {
                        syncWechatInfo(weChatInfo);
                        break;
                    } catch (Exception e) {
                        retryTimes++;
                        if (retryTimes == 3) {
                            k++;
                            failedList.add(weChatInfo.getWxUnionId());
                            e.printStackTrace();
                            logger.error("用户信息同步失败:" + weChatInfo.getWxUnionId(), e);
                        }
                    }
                }
            }

            i += apiResponse.getPagedData().size();
            System.out.println("同步完成" + i + "条数据" + " 手机号不正确忽略" + j + "条数据" + " 同步失败" + k + "条数据");
            if (failedList.size() > 0) {
                System.out.println("出错用户ID : " + StringUtils.join(failedList, ","));
            }


            if (apiResponse.getCount() < requestPage.getPageSize()) {
                break;
            }

            requestPage.pagingNext();
        }
        System.out.println("全部同步完成, 总共 ： " + i);
    }

    private void syncWechatInfo(WeChatInfo weChatInfo) throws Exception {
        AnonymousAccount anonymousAccount = new AnonymousAccount();
        anonymousAccount.setType(AnonymousType.WECHAT);
        anonymousAccount.setObjectId(weChatInfo.getWxUnionId());
        anonymousAccount.setCreatedTime(weChatInfo.getCreatedTime());
        anonymousAccount.setUpdatedTime(weChatInfo.getUpdatedTime());
        List<UserWeChatRelation> userWeChatRelations = userWeChatRelationApiService.findAll(ApiRequest.newInstance().filterEqual(QUserWeChatRelation.wxUnionId, weChatInfo.getWxUnionId()));
        Collections.sort(userWeChatRelations, (o1, o2) ->  Long.compare(o1.getCreatedTime().getTime(), o2.getCreatedTime().getTime()));
        Oauth2Account oauth2Account = null;
        for (int i = 0; i < userWeChatRelations.size(); i++) {
            UserWeChatRelation userWeChatRelation = userWeChatRelations.get(i);
            String clientId = convertClientId(userWeChatRelation.getSourceType());
            if (i == 0) {
                anonymousAccount.setRegisterClientId(clientId);
            }
            oauth2Account = userAccountApiService.createAccount(anonymousAccount, clientId, userWeChatRelation.getWxOpenId(), userWeChatRelation.getUserOpenId());
        }
        WechatAccountDetails wechatAccountDetails = new WechatAccountDetails();
        wechatAccountDetails.setNickName(weChatInfo.getNickName());
        wechatAccountDetails.setHeadUrl(weChatInfo.getIconUrl());
        wechatAccountDetails.setSexType(SexType.valueOf(weChatInfo.getSex().toString()));
        wechatAccountDetails.setCity(weChatInfo.getCity());
        wechatAccountDetails.setCountry(weChatInfo.getCountry());
        wechatAccountDetails.setLanguage(weChatInfo.getLanguage());
        wechatAccountDetails.setProvince(weChatInfo.getProvince());
        wechatAccountDetails.setUnionId(weChatInfo.getWxUnionId());
        userAccountApiService.updateWechatAccountDetails(wechatAccountDetails);
        if (weChatInfo.getUserId() != null && weChatInfo.getUserId() != 0L) {
            UserInfo userInfo = userInfoApiService.findUserByUserId(weChatInfo.getUserId());
            if (userInfo != null && oauth2Account != null) {
                UserAccount userAccount = userAccountApiService.getByPhone(userInfo.getPhone());
                userAccountApiService.bindUserAccount(oauth2Account.getAccountId(), userAccount.getId());
            }
        }
    }

    private void syncUserInfo(UserInfo userInfo) throws Exception {
        UserAccount userAccount = new UserAccount();
        userAccount.setPhone(userInfo.getPhone());
        userAccount.setUserKey(USER_KEY_PREFIX + userInfo.getPhone());
        userAccount.setSalt(MD5Util.encoderByMd5(UUID.randomUUID().toString()));
        userAccount.setSecret(MD5Util.encoderByMd5(MD5Util.encoderByMd5(CoreStringUtils.generateRandomStr(6)) + userAccount.getSalt()));
        userAccount.setCreatedTime(userInfo.getCreatedTime());
        userAccount.setUpdatedTime(userInfo.getUpdatedTime());
        if (userInfo.getRegisterType().equals(UserRegisterType.APP)) {
            userAccount.setRegisterClientId("sqbj-server");
        } else {
            userAccount.setRegisterClientId("sqbj-news-small");
        }
        Oauth2Account oauth2Account = userAccountApiService.createAccount(userAccount, "sqbj-server", userInfo.getUserOpenId());

        UserAccountDetails userAccountDetails = userAccountApiService.getUserAccountDetails(oauth2Account.getAccountId());
        userAccountDetails.setBirthday(userInfo.getBirthday());
        userAccountDetails.setHeadUrl(userInfo.getIconUrl());
        userAccountDetails.setAgeGroup(userInfo.getAgeGroup());
        userAccountDetails.setSexType(SexType.valueOf(userInfo.getSex().toString()));
        userAccountDetails.setRealName(userInfo.getRealName());
        userAccountDetails.setNickName(userInfo.getNickName());
        userAccountDetails.setPhone(userInfo.getPhone());
        userAccountDetails.setAccountId(oauth2Account.getAccountId());

        userAccountApiService.updateUserAccountDetails(userAccountDetails);

        List<UserDeviceRelationship> list = userDeviceRelationshipApiService.findByUserId(userInfo.getUserId(), ClientType.SQBJ);

        UserDeviceRelationship bindUserDeviceRelationship = null;
        for (UserDeviceRelationship userDeviceRelationship : list) {
            if (userDeviceRelationship.getIsActivity().equals(YesNoStatus.YES)) {
                bindUserDeviceRelationship = userDeviceRelationship;
                continue;
            }
            ClientDevice clientDevice = deviceApiService.getClientDevice(userDeviceRelationship.getClientType(), userDeviceRelationship.getClientId());
            if (clientDevice != null) {
                DeviceType deviceType = DeviceType.ANDROID;
                if (clientDevice.getClientOSType().equals(ClientOSType.IOS)) {
                    deviceType = DeviceType.IPHONE;
                }
                MobileDevice mobileDevice = clientDeviceApiService.getMobileDevice(userDeviceRelationship.getClientId(), "sqbj-server", deviceType);
                if (mobileDevice != null) {
                    userAccountApiService.bindDevice(oauth2Account.getAccountId(), deviceType, mobileDevice.getClientDeviceId(), "sqbj-server");
                }
            }
        }

        if (bindUserDeviceRelationship != null) {
            ClientDevice clientDevice = deviceApiService.getClientDevice(bindUserDeviceRelationship.getClientType(), bindUserDeviceRelationship.getClientId());
            if (clientDevice != null) {
                DeviceType deviceType = DeviceType.ANDROID;
                if (clientDevice.getClientOSType().equals(ClientOSType.IOS)) {
                    deviceType = DeviceType.IPHONE;
                }
                MobileDevice mobileDevice = clientDeviceApiService.getMobileDevice(bindUserDeviceRelationship.getClientId(), "sqbj-server", deviceType);
                if (mobileDevice != null) {
                    userAccountApiService.bindDevice(oauth2Account.getAccountId(), deviceType, mobileDevice.getClientDeviceId(), "sqbj-server");
                }
            }
        }
    }

    private String convertClientId(WeChatOpenIdType weChatOpenIdType) {
        if (weChatOpenIdType.equals(WeChatOpenIdType.APP_CLIENT)) {
            return "sqbj-server";
        } else  {
            return "sqbj-news-small";
        }
    }

    private SexType convertSex(String gender) {
        if (gender == null) {
            return SexType.Unknown;
        }
        switch (gender) {
            case "0" : return SexType.Unknown;
            case "1" : return SexType.Male;
            case "2" : return SexType.Female;
        }
        return SexType.Unknown;
    }
}
