package com.bg.dwd.function;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * @Package com.bg.dwd.function.birthdayAgeFun
 * @Author Chen.Run.ze
 * @Date 2025/5/12 19:58
 * @description: 根据出生日期计算实际年龄
 */
public class BirthdayAgeFun {
    public static int calculateAge(String birthDateStr) throws DateTimeParseException {
        // 1. 定义日期格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        // 2. 解析日期
        LocalDate birthDate = LocalDate.parse(birthDateStr, formatter);
        LocalDate currentDate = LocalDate.now();

        // 3. 计算基础年龄
        int age = Period.between(birthDate, currentDate).getYears();

        // 4. 处理闰年特殊日期（2月29日）
        if (birthDate.getMonthValue() == 2 && birthDate.getDayOfMonth() == 29) {
            if (!currentDate.isLeapYear() && currentDate.getMonthValue() == 2 && currentDate.getDayOfMonth() == 28) {
                // 当前是非闰年2月28日，视为已过生日
                return age;
            }
        }
        // 5. 调整未过生日的情况
        if (currentDate.getMonthValue() < birthDate.getMonthValue() ||
                (currentDate.getMonthValue() == birthDate.getMonthValue() &&
                        currentDate.getDayOfMonth() < birthDate.getDayOfMonth())) {
            age--;
        }

        return age;
    }
}
