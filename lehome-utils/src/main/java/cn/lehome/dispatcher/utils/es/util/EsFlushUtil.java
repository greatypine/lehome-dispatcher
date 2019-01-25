package cn.lehome.dispatcher.utils.es.util;

import cn.lehome.dispatcher.utils.es.client.EsClient;
import cn.lehome.framework.base.api.core.exception.common.ParamValidException;
import cn.lehome.framework.base.api.core.service.impl.AbstractElasticsearchBaseApiServiceImpl;
import cn.lehome.framework.bean.core.entity.BaseEntity;
import cn.lehome.framework.bean.core.enums.YesNoStatus;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.Consumer;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.util.CollectionUtils;

import javax.persistence.Id;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhanghuan on 2018/7/17.
 */
public class EsFlushUtil <T extends BaseEntity> extends AbstractElasticsearchBaseApiServiceImpl{

    private Client client;

    public static EsFlushUtil getInstance(){
        return new EsFlushUtil();
    }

    private EsFlushUtil(){
        client = EsClient.getEsClient().getClient();
    }

    private static final Long INVALID_DAY =365L;


    /**
     * 批量插入
     * @param docs
     * @return 插入个数
     */
    public int batchInsert(List<T> docs) {
        if (CollectionUtils.isEmpty(docs)) {
            throw new ParamValidException("EsIndexEntity集合不能为空");
        }
        System.out.println("需要插入数据: "+docs.size()+"条");
        //构建批量插入
        Consumer<BulkRequestBuilder> buildRequestFunction = bulkRequestBuilder -> {
            for (T doc : docs) {
                Document document = doc.getClass().getAnnotation(Document.class);
                String json = JSON.toJSONString(doc);
                String id = readIdValue(doc);
                bulkRequestBuilder.add(client.prepareIndex(document.indexName(), document.type(),id).setSource(json).
                        setTTL(new TimeValue(INVALID_DAY, TimeUnit.DAYS)));
            }
        };
        executeBulk(buildRequestFunction);
        return docs.size();
    }


    /**
     * 批量更新
     * @param docs
     * @return 更新个数
     */
    public int batchUpdate(List<T> docs) {
        if (CollectionUtils.isEmpty(docs)) {
            throw new ParamValidException("EsIndexEntity集合不能为空");
        }
        System.out.println("需要更新数据: "+docs.size()+"条");
        //构建批量更新
        Consumer<BulkRequestBuilder> buildRequestFunction = bulkRequestBuilder -> {
            for (T doc : docs) {
                Document document = doc.getClass().getAnnotation(Document.class);
                String id = readIdValue(doc);
                String json = JSON.toJSONString(doc);
                bulkRequestBuilder.add(client.prepareUpdate(document.indexName(), document.type(),id)
                        .setDoc(json));
            }
        };
        executeBulk(buildRequestFunction);
        return docs.size();
    }


    /**
     * 批量删除
     * @param docs
     * @return
     */
    public int batchDelete(List<T> docs) {
        if (docs == null || docs.isEmpty()) {
            throw new ParamValidException("EsIndexEntity集合不能为空");
        }
        //构建批量更新
        Consumer<BulkRequestBuilder> buildRequestFunction = bulkRequestBuilder -> {
            for (T doc : docs) {
                Document document = doc.getClass().getAnnotation(Document.class);
                String id = readIdValue(doc);
                bulkRequestBuilder.add(client.prepareDelete(document.indexName(), document.type(), id));
            }
        };
        executeBulk(buildRequestFunction);
        return docs.size();
    }


    /**
     * 批量删除子索引
     * @param docs 子索引集合
     * @param parentId 父索引ID
     * @return 删除数量
     */
    public int deleteChild(List<T> docs,String parentId) {
        if (docs == null || docs.isEmpty()) {
            throw new ParamValidException("EsIndexEntity集合不能为空");
        }
        //构建批量更新
        Consumer<BulkRequestBuilder> buildRequestFunction = bulkRequestBuilder -> {
            for (T doc : docs) {
                Document document = doc.getClass().getAnnotation(Document.class);
                String id = readIdValue(doc);
                DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete(document.indexName(),document.type(),id);
                deleteRequestBuilder.setRouting(parentId);
                bulkRequestBuilder.add(deleteRequestBuilder);
            }
        };
        executeBulk(buildRequestFunction);
        return docs.size();
    }

    /**
     * 批量插入
     * @param docs
     * @return 插入个数
     */
    public int batchInsertChild(List<T> docs,String parentFiedName) {
        if (CollectionUtils.isEmpty(docs)) {
            throw new ParamValidException("EsIndexEntity集合不能为空");
        }
        System.out.println("需要插入数据: "+docs.size()+"条");
        //构建批量插入
        Consumer<BulkRequestBuilder> buildRequestFunction = bulkRequestBuilder -> {
            for (T doc : docs) {
                Document document = doc.getClass().getAnnotation(Document.class);
                String json = JSON.toJSONString(doc);
                String id = readIdValue(doc);
                String parentId = readFiedValue(doc,parentFiedName);
                System.out.println();
                bulkRequestBuilder.add(client.prepareIndex(document.indexName(), document.type(),id).setSource(json).
                        setTTL(new TimeValue(INVALID_DAY, TimeUnit.DAYS)).setRouting(parentId));
            }
        };
        executeBulk(buildRequestFunction);
        return docs.size();
    }


    /**
     * 执行批量操作
     *
     * @param buildRequestFunction 构建处理
     */
    private void executeBulk(Consumer<BulkRequestBuilder> buildRequestFunction) {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        buildRequestFunction.accept(bulkRequestBuilder);
        BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
        bulkRequestBuilder.request().requests().clear();
        if (bulkResponse.hasFailures()) {
            throw new RuntimeException(bulkResponse.buildFailureMessage());
        }else {
            System.out.println("刷新es数据完毕");
        }
    }


    /**
     * 通过反射获取id属性值
     * @param obj
     * @return value
     */
    private static String readIdValue(Object obj){
        String idVlues="";
        //得到class
        Class cls = obj.getClass();
        //得到所有属性
        Field[] fields = cls.getDeclaredFields();
        //得到属性 默认取第一个字段为ID属性
        Field field = fields[0];
        for (Field f : fields) {
            Id annotation = f.getAnnotation(Id.class);
            if (annotation != null) {
                field = f;
                break;
            }
        }
        //打开私有访问
        field.setAccessible(true);
        //获取属性值
        Object value = null;
        try {
            value = field.get(obj);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        idVlues =String.valueOf(value);
        return idVlues;
    }



    /**
     * 通过反射获取id属性值
     * @param obj
     * @return value
     */
    private static String readFiedValue(Object obj,String fieldName){
        String idVlues="";
        //得到class
        Class cls = obj.getClass();
        //得到所有属性
        Field[] fields = cls.getDeclaredFields();
        //得到属性 默认取第一个字段为ID属性
        Field field = fields[0];
        for (Field f : fields) {
            if (f.getName().equals(fieldName)){
                field = f;
                break;
            }
        }
        //打开私有访问
        field.setAccessible(true);
        //获取属性值
        Object value = null;
        try {
            value = field.get(obj);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        idVlues =String.valueOf(value);
        return idVlues;
    }


    /**
     * 通过scroll api 进行翻页查询es数据
     * @param cs 文档实体class
     * @param pageSize 每页大小
     * @param queryBuilders 查询参数
     * @param millis scrollId超时时间
     * @return 本次查询游标和本页数据
     */
    public EsScrollResponse<T> searchByScroll(Class<T> cs, Integer pageSize, QueryBuilders queryBuilders, long millis ) {
        Document annotation = cs.getAnnotation(Document.class);
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        searchRequestBuilder.setIndices(annotation.indexName());
        searchRequestBuilder.setTypes(annotation.type());
        searchRequestBuilder.setScroll(new TimeValue(millis));
        searchRequestBuilder.setSize(pageSize);
        //searchRequestBuilder.setQuery(queryBuilders.matchAllQuery());

        SearchResponse searchResponse = searchRequestBuilder.get();
        String scrollId = searchResponse.getScrollId();
        System.out.println("----searchByScroll scrollId=" + scrollId);

        SearchHit[] searchHits = searchResponse.getHits().getHits();
        List<T> list = Lists.newArrayListWithCapacity(pageSize);
        for (SearchHit searchHit : searchHits) {
            T data = JSON.parseObject(JSON.toJSONString(searchHit.getSource()), cs);
            if (data == null || readIdValue(data) == null) {
                continue;
            }
            list.add(data);
        }
        return new EsScrollResponse<>(scrollId, list);
    }

    /**
     * 通过上次翻页得到的scrollId进行翻页查询es数据
     * @param cs 文档实体class
     * @param pageSize 每页大小
     * @param millis scrollId超时时间
     * @return 本次查询游标和本页数据
     */
    public EsScrollResponse<T> searchByScrollId(Class<T> cs, Integer pageSize, String scrollId, long millis ) {
        SearchScrollRequestBuilder searchScrollRequestBuilder;
        SearchResponse searchResponse;

        //System.out.println("-------searchByScrollId scrollId " + scrollId);
        searchScrollRequestBuilder = client.prepareSearchScroll(scrollId);
        searchScrollRequestBuilder.setScroll(new TimeValue(millis));
        searchResponse = searchScrollRequestBuilder.get();

        if (searchResponse.getHits().getHits().length == 0) {
            return null;
        }
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        List<T> list = Lists.newArrayListWithCapacity(pageSize);
        for (SearchHit searchHit : searchHits) {
            T data = JSON.parseObject(JSON.toJSONString(searchHit.getSource()), cs);
            if (data == null || readIdValue(data) == null) {
                continue;
            }
            list.add(data);
        }
        return new EsScrollResponse<>(searchResponse.getScrollId(), list);
    }

    /**
     * 批量清除scrollId
     */
    public boolean clearScrollIds(List<String> scrollIds) {
        ClearScrollRequestBuilder builder = client.prepareClearScroll();
        builder.setScrollIds(scrollIds);
        ClearScrollResponse clearScrollResponse = builder.get();
        return clearScrollResponse.isSucceeded();
    }

    /**
     * 清除scrollId
     */
    public boolean clearScrollId(String scrollIds) {
        ClearScrollRequestBuilder builder = client.prepareClearScroll();
        builder.addScrollId(scrollIds);
        ClearScrollResponse clearScrollResponse = builder.get();
        return clearScrollResponse.isSucceeded();
    }

    public Client getClient() {
        return client;
    }

    public void closeClient() {
        client.close();
    }




    /**
     * 通过scroll api 进行翻页查询es数据
     * @param cs 文档实体class
     * @param pageSize 每页大小
     * @param queryBuilders 查询参数
     * @param millis scrollId超时时间
     * @return 本次查询游标和本页数据
     */
    public EsScrollResponse<T> searchByScrollTemp(Class<T> cs, Integer pageSize, QueryBuilders queryBuilders, long millis , YesNoStatus y) {
        Document annotation = cs.getAnnotation(Document.class);
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch();
        searchRequestBuilder.setIndices("goods_info4");
        if (YesNoStatus.YES.equals(y)){
            searchRequestBuilder.setTypes("goods_spu");
        }else {
            searchRequestBuilder.setTypes("goods_sku");
        }

        searchRequestBuilder.setScroll(new TimeValue(millis));
        searchRequestBuilder.setSize(pageSize);
        //searchRequestBuilder.setQuery(queryBuilders.matchAllQuery());

        SearchResponse searchResponse = searchRequestBuilder.get();
        String scrollId = searchResponse.getScrollId();
        System.out.println("----searchByScroll scrollId=" + scrollId);

        SearchHit[] searchHits = searchResponse.getHits().getHits();
        List<T> list = Lists.newArrayListWithCapacity(pageSize);
        for (SearchHit searchHit : searchHits) {
            T data = JSON.parseObject(JSON.toJSONString(searchHit.getSource()), cs);
            if (data == null || readIdValue(data) == null) {
                continue;
            }
            list.add(data);
        }
        return new EsScrollResponse<>(scrollId, list);
    }

    /**
     * 通过上次翻页得到的scrollId进行翻页查询es数据
     * @param cs 文档实体class
     * @param pageSize 每页大小
     * @param millis scrollId超时时间
     * @return 本次查询游标和本页数据
     */
    public EsScrollResponse<T> searchByScrollIdTemp(Class<T> cs, Integer pageSize, String scrollId, long millis ) {
        SearchScrollRequestBuilder searchScrollRequestBuilder;
        SearchResponse searchResponse;

        //System.out.println("-------searchByScrollId scrollId " + scrollId);
        searchScrollRequestBuilder = client.prepareSearchScroll(scrollId);
        searchScrollRequestBuilder.setScroll(new TimeValue(millis));
        searchResponse = searchScrollRequestBuilder.get();

        if (searchResponse.getHits().getHits().length == 0) {
            return null;
        }
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        List<T> list = Lists.newArrayListWithCapacity(pageSize);
        for (SearchHit searchHit : searchHits) {
            T data = JSON.parseObject(JSON.toJSONString(searchHit.getSource()), cs);
            if (data == null || readIdValue(data) == null) {
                continue;
            }
            list.add(data);
        }
        return new EsScrollResponse<>(searchResponse.getScrollId(), list);
    }
}
