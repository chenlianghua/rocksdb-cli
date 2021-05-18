package org.geye.rocksdbCli;

import java.util.HashMap;

public class ExpressionParser {

    public ExpressionParser() {

    }

    public void parse(String expression) {



    }
}

enum OperatorEnum {
    //!,&,|运算时暂时未考虑在内，主要用于判断字符是否为操作
    NOT("!",0),
    LT("<",1),
    ELT("<=",1),
    GT(">",1),
    EGT(">=",1),
    EQ("==",2),
    NEQ("!=",2),
    BAND("&", 3),
    BOR("|", 4),
    AND("&&",5),
    OR("||",6),
    E("=", 7);

    private String name;
    private Integer priority;

    OperatorEnum(String name, Integer priority){
        this.name = name;
        this.priority = priority;
    }

    private static HashMap<String, OperatorEnum> enums = new HashMap<>();
    public static void enumToMap(){
        enums.put("!", OperatorEnum.NOT);
        enums.put("<", OperatorEnum.LT);
        enums.put("<=", OperatorEnum.ELT);
        enums.put(">", OperatorEnum.GT);
        enums.put(">=", OperatorEnum.EGT);
        enums.put("==", OperatorEnum.EQ);
        enums.put("!=", OperatorEnum.NEQ);
        enums.put("&", OperatorEnum.BAND);
        enums.put("|", OperatorEnum.BOR);
        enums.put("&&", OperatorEnum.AND);
        enums.put("||", OperatorEnum.OR);
        enums.put("=", OperatorEnum.E);
    }

    public static OperatorEnum getEnumByName(String name){
        if(enums.size() < 1){
            enumToMap();
        }
        return enums.get(name);
    }

    public static boolean isOperator(String name){
        if(enums.size() < 1){
            enumToMap();
        }
        return enums.containsKey(name);
    }

    public Integer getPriority() {
        return priority;
    }

    public String getName() {
        return name;
    }

}