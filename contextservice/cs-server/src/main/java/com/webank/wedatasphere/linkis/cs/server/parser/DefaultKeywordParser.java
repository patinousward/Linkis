package com.webank.wedatasphere.linkis.cs.server.parser;

import com.webank.wedatasphere.linkis.cs.common.annotation.KeywordMethod;
import com.webank.wedatasphere.linkis.cs.server.conf.ContextServerConf;
import org.apache.commons.lang.StringUtils;
import org.reflections.ReflectionUtils;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author peacewong
 * @date 2020/2/9 16:19
 */
@Component
public class DefaultKeywordParser implements KeywordParser {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKeywordParser.class);


    Map<String, Set<KeywordMethodEntity>> keywordMethods = new HashMap<>();

    Map<String, Set<Class<?>>> classRecord = new HashMap<>();


    @PostConstruct
    private void init() {
        logger.info("init keyValueParser");
        scanKeywordMethods();
    }

    private void scanKeywordMethods() {
        Reflections reflections = new Reflections(ContextServerConf.KEYWORD_SCAN_PACKAGE, new MethodAnnotationsScanner());
        Set<Method> methods = reflections.getMethodsAnnotatedWith(KeywordMethod.class);
        Iterator<Method> iterator = methods.iterator();
        while (iterator.hasNext()) {
            Method method = iterator.next();
            method.setAccessible(true);

            KeywordMethod annotation = method.getAnnotation(KeywordMethod.class);
            KeywordMethodEntity keywordMethodEntity = new KeywordMethodEntity();
            keywordMethodEntity.setMethod(method);
            keywordMethodEntity.setRegex(annotation.regex());
            keywordMethodEntity.setSplitter(annotation.splitter());

            String className = method.getDeclaringClass().getName();
            if (!keywordMethods.containsKey(className)) {
                keywordMethods.put(className, new HashSet<>());
            }
            keywordMethods.get(className).add(keywordMethodEntity);
        }
    }

    /**
     *
     * @param obj
     * @return
     * @throws Exception
     */
    private Set<String> parseKeywords(Object obj) throws Exception {
        Objects.requireNonNull(obj);
        Set<String> keywords = new HashSet<>();
        String className = obj.getClass().getName();
        if (!classRecord.containsKey(className)) {
            classRecord.put(className, ReflectionUtils.getAllSuperTypes(obj.getClass()));
        }

        Iterator<Class<?>> classIterator = classRecord.get(className).iterator();
        while (classIterator.hasNext()) {
            Class<?> clazz = classIterator.next();
            if (! keywordMethods.containsKey(clazz.getName())) {
                continue;
            }
            Iterator<KeywordMethodEntity> keywordMethodEntityIterator = keywordMethods.get(clazz.getName()).iterator();
            while (keywordMethodEntityIterator.hasNext()) {
                KeywordMethodEntity methodEntity = keywordMethodEntityIterator.next();
                Object methodReturn = methodEntity.getMethod().invoke(obj);
                if (null == methodReturn || StringUtils.isBlank(methodReturn.toString())) {
                    continue;
                }
                if (StringUtils.isNotBlank(methodEntity.getSplitter())) {
                    Collections.addAll(keywords, methodReturn.toString().split(methodEntity.getSplitter()));
                } else if (StringUtils.isNotBlank(methodEntity.getRegex())) {
                    keywords.addAll(getString(methodReturn.toString(), methodEntity.getRegex()));
                } else {
                    keywords.add(methodReturn.toString());
                }
            }
        }
        return keywords;
    }

    @Override
    public Set<String> parse(Object obj) {
        //先解析key
        Set<String> keywords = new HashSet<>();
        try {
            keywords =  parseKeywords(obj);
        } catch (Exception e){
           logger.error("Failed to parse keywords ", e);
        }
        return keywords;
    }

    private  Set<String> getString(String s, String regex) {

        Set<String> keywords = new HashSet<>();
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(s);
        while(m.find()) {
            keywords.add(m.group());

        }
        return keywords;
    }
}
