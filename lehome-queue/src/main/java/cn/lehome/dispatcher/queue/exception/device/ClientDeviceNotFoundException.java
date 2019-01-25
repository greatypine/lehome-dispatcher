package cn.lehome.dispatcher.queue.exception.device;

import cn.lehome.framework.base.api.core.annotation.exception.ExceptionCode;
import cn.lehome.framework.base.api.core.exception.BaseApiException;

@ExceptionCode(code = "DQ0001", desc = "设备信息未找到")
public class ClientDeviceNotFoundException extends BaseApiException {

    private static final long serialVersionUID = 9205075198518367788L;

}
