package cn.lehome.dispatcher.utils.user;

/**
 * Created by wuzhao on 2018/12/4.
 */
public interface UserSync {

    void sync(Long startId);

    void wechatSync(String wxUnionId);
}
