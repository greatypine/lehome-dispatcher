package cn.lehome.dispatcher.utils.operation;

/**
 * Created by zhanghuan on 2018/10/26.
 */
public interface OperationService {

    void migrateData(String [] input);

    void flushIconDate();
}
