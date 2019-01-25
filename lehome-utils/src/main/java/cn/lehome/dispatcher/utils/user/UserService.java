package cn.lehome.dispatcher.utils.user;

/**
 * Created by zuoguodong on 2018/3/22
 */
public interface UserService {

    /**
     * 刷新用户缓存信息
     */
    void refreshUserCache(String[] input);

    void refreshUserCacheByPhone(String[] input);

    void kickUser(String[] input);

    void repairUserCache();

    void userInformationStatistics();

    /**
     * 刷新用户附加属性索引信息
     */
    void initUserAdditionalIndex(String[] input);

    void saveUserToHbase();

    void repairUserIndexWithDB(String[] input);

    /**
     * 刷新微信用户索引数据
     */
    void updateWeChatIndex(String[] input);

    void updateUserRegisterSourceIndex(String[] input);

    void updateUserIndexFromExcel(String[] input);
}
