package cn.lehome.dispatcher.utils.es.exception;

/**
 * Created by zhanghuan on 2018/7/19.
 */
public class ClientInitException extends RuntimeException {
    public ClientInitException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
