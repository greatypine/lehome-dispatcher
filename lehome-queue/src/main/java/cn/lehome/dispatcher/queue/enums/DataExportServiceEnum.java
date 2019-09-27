package cn.lehome.dispatcher.queue.enums;

import cn.lehome.base.api.common.constant.ExportDataBusinessConstants;

/**
 * Created by zuoguodong on 2019/9/26
 */
public enum DataExportServiceEnum {

    ORDER_BACK(ExportDataBusinessConstants.ORDER_BACK,"orderBackDataExportService");

    DataExportServiceEnum(String key,String name){
        this.key = key;
        this.name = name;
    }

    private String key;

    private String name;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static String getServiceName(String key){
        for(DataExportServiceEnum d : DataExportServiceEnum.values()){
            if(d.key.equals(key)){
                return d.name;
            }
        }
        return null;
    }
}
