package com.lzf.qzpoint.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class numberTest {
    public static void main(String[] args) {
        String t = "1234aaa.qweqweqwe.";
        String regEx_special = "(^[0-9]*)|([.]*)";
        Pattern compile = Pattern.compile(regEx_special);
        Matcher matcher = compile.matcher(t);
        String result=matcher.replaceAll("");
        System.out.println(result);
    }
}
