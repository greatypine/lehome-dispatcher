package cn.lehome.dispatcher.queue.service.token;


import cn.lehome.base.api.common.bean.community.CommunityExt;

/**
 * Created by wuzhao on 2018/3/19.
 */
public interface SmartExchangeTokenService {

    void exchangeAll(String userOpenId);

    void refreshAll(String userOpenId);

    void exchangeSmartToken(String userOpenId, String uniqueCode);

    void oldExchangeSmartToken(String userOpenId, CommunityExt communityExt);
}
