package cn.lehome.dispatcher.queue.service.push;

import cn.lehome.base.api.tool.bean.push.PushPlan;
import cn.lehome.base.api.user.bean.user.UserInfoIndex;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface PushService {

    List<Long> readExcelUserInfo(String ossUrl);

    Set<String> readExcelUserInfoMobiles(String ossUrl);

    Map<String,List<UserInfoIndex>> findAllUserInfo(PushPlan pushPlan);
}


