package cn.lehome.dispatcher.queue.exception.device;

import cn.lehome.framework.base.api.core.annotation.exception.ExceptionCode;
import cn.lehome.framework.base.api.core.exception.BaseApiException;

@ExceptionCode(code = "DQ0003", desc = "用户与设备信息关系未找到")
public class UserClientRelationshipNotFoundException extends BaseApiException {

    private static final long serialVersionUID = 9205075198518367788L;

}
