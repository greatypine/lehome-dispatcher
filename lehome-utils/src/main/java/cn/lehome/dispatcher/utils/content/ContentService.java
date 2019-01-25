package cn.lehome.dispatcher.utils.content;

import java.text.ParseException;

/**
 * Created by zuoguodong on 2018/4/3
 */
public interface ContentService {

    void initContentIndex(String input[]);

    void updatePostInfoIndexByMd5Key(String input[]);

    void deleteRepeatPost();

    void coverExtensionToPost(String input[]);

    void repairExtensionRelationWithType(String[] input);

    void createCancelTopPostTask();

    void updatePostSelectedStatus(String[] input) throws ParseException;

    void updateCommentIsAnonymousStatus(String[] input);

    void updatePostCommunityId(String[] input);
}
