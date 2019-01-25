package cn.lehome.dispatcher.queue.exception.task;

import cn.lehome.framework.base.api.core.annotation.exception.ExceptionCode;
import cn.lehome.framework.base.api.core.exception.BaseApiException;

/**
 * Created by zuoguodong on 2018/5/16
 */
@ExceptionCode(code = "UT0001", desc = "用户任务处理出错",recoverDesc = true)
public class UserTaskException extends BaseApiException {

    public UserTaskException(String msg){
        super(msg);
    }

}
