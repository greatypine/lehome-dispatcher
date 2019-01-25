package cn.lehome.dispatcher.queue.exception.device;

import cn.lehome.framework.base.api.core.annotation.exception.ExceptionCode;
import cn.lehome.framework.base.api.core.exception.BaseApiException;

@ExceptionCode(code = "DQ0002", desc = "推送信息未找到")
public class PushDeviceInfoNotFoundException extends BaseApiException {

    private static final long serialVersionUID = 9205075198518367788L;

}
