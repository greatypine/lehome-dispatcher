package cn.lehome.dispatcher.utils.es.config;

import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Map;

/**
 * Created by zhanghuan on 2018/7/19.
 */
@Configuration
public class EsConfig {

    @Autowired
    private Environment environment;

    public static Map<String,String> CONFIG_MAP = null;

    @Bean
    public String initEsConfig(){
        CONFIG_MAP = new ImmutableMap.Builder<String,String>().
                put("clusterNodes",environment.getProperty("elasticsearch.clusterNodes")).
                put("clusterName",environment.getProperty("elasticsearch.clusterName")).
                build();
        return "ok";
    }

}