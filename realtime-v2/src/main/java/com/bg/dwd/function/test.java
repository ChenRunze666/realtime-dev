package com.bg.dwd.function;

import com.ibm.icu.text.SimpleDateFormat;

import java.text.DateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @Package com.bg.dwd.function.test
 * @Author Chen.Run.ze
 * @Date 2025/5/12 19:35
 * @description: 1
 */
public class test {
    public static void main(String[] args) {
        String format = new SimpleDateFormat("yyyyMMdd").format(new Date());
        System.out.println(format);
    }
}
