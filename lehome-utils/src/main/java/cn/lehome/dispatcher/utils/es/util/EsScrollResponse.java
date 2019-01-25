package cn.lehome.dispatcher.utils.es.util;

import java.io.Serializable;
import java.util.List;

public class EsScrollResponse<T> implements Serializable {

    private static final long serialVersionUID = -7708215112534966105L;

    private String scrollId;

    private List<T> datas;

    public EsScrollResponse() {

    }

    public EsScrollResponse(String scrollId, List<T> list) {
        this.scrollId = scrollId;
        this.datas = list;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    public List<T> getDatas() {
        return datas;
    }

    public void setDatas(List<T> datas) {
        this.datas = datas;
    }
}
