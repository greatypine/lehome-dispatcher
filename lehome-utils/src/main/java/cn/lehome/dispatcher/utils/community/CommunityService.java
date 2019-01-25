package cn.lehome.dispatcher.utils.community;

/**
 * Created by zuoguodong on 2018/3/20
 */
public interface CommunityService {

    /**
     * 小区附加信息绑定
     * @param args
     */
    void bindExt(String[] args);

    /**
     * 修改小区名称
     * @param args
     */
    void updateCommunityExtName(String[] args);

    /**
     * 初始化小区贴子信息
     */
    void initPost(String[] input);

    /**
     * 清除机器人发的贴子信息
     */
    void cleanPost();

    void updateCommunityUniqueCode(String accessToken);

    void updateAllCommunityExt();

    void updateAllCommunity();
}
