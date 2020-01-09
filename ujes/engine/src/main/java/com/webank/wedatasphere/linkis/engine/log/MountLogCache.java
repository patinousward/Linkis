/*
 * Copyright 2019 WeBank
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.wedatasphere.linkis.engine.log;

import com.webank.wedatasphere.linkis.engine.conf.EngineConfiguration$;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * created by enjoyyin on 2018/11/27
 * Description:
 */
//正真使用的LogCache
public class MountLogCache extends AbstractLogCache{

    private static final Logger logger = LoggerFactory.getLogger(MountLogCache.class);
    //环形数组用来防止日志大量的时候造成的oom
    class CircularQueue{

        private int max;
        private String[] elements;
        private int front, rear, count;
        CircularQueue(){
            this((Integer)EngineConfiguration$.MODULE$.ENGINE_LOG_CACHE_NUM().getValue());
        }

        CircularQueue(int max){
            this.max = max;
            this.elements = new String[max];
        }

        public boolean isEmpty(){
            return count == 0;
        }

        public synchronized void enqueue(String value){
            if(count == max){
                logger.warn("Queue is full, log: {} needs to be dropped", value);
            }else{
                rear = (rear + 1) % max;
                elements[rear] = value;
                count ++;
            }
        }

        public String dequeue(){
            if (count == 0){
                logger.debug("Queue is empty, nothing to get");
                return null;
            }else{
                front = (front + 1) % max;
                count --;
                return elements[front];
            }
        }

        public List<String> dequeue(int num){
            List<String> list = new ArrayList<>();
            int index = 0;
            while(index < num){
                String tempLog = dequeue();
                if(StringUtils.isNotEmpty(tempLog)){
                    list.add(tempLog);
                }else if (tempLog == null){
                    break;
                }
                index ++;
            }
            return list;
        }

        public synchronized List<String> getRemain(){
            List<String> list = new ArrayList<>();
            while(!isEmpty()){
                list.add(dequeue());
            }
            return list;
        }

        public int size(){
            return count;
        }
    }


    private CircularQueue logs;


    //创建cache对象的时候进行创建环形数组
    public MountLogCache(int loopMax){
        this.logs = new CircularQueue(loopMax);
    }

    @Override
    public void cacheLog(String log) {
        logs.enqueue(log);
    }//入队列

    @Override
    public List<String> getLog(int num) {
        return logs.dequeue(num);
    }//出出列

    @Override
    public List<String> getRemain() {
        return logs.getRemain();
    }//获取剩余的日志

    @Override
    public int size() {
        return logs.size();
    }
}
