package com.bg.sensitive_words.untis;

import redis.clients.jedis.Jedis;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
/**
 * @Package com.bg.sensitive_words.untis.WriteFileToRedis
 * @Author Chen.Run.ze
 * @Date 2025/5/8 21:27
 * @description: 将文件数据写入Redis
 */
public class WriteFileToRedis {
    public static void main(String[] args) {
        String file_path = "D:\\DaShuJu2\\workspace\\IdeaDemo\\realtime-dev\\realtime-stream\\src\\main\\resources\\Identify-sensitive-words.txt";
        String redis_key = "sensitive_words";
        try (Jedis jedis = new Jedis("cdh03", 6379);
             BufferedReader reader = new BufferedReader(new FileReader(file_path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 使用 SADD 命令将每行敏感词作为元素添加到集合类型的 key 中
                jedis.sadd(redis_key, line);
                System.out.println("One sensitive word has been added to Redis with key: " + redis_key);
            }
            System.out.println("All sensitive words from the file have been added to Redis.");
        } catch (IOException e) {
            System.out.println("Error: The file was not found.");
        } catch (Exception e) {
            System.out.println("An error occurred: " + e.getMessage());
        }
    }
}
