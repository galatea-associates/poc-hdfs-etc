package org.galatea.pochdfs.utils.hdfs;

public class CashflowRegexBuilder {

  public static String build(String date, long id){
    date = date.replaceAll("-","");
    int year = Integer.parseInt(date.substring(0,4));
    int month = Integer.parseInt(date.substring(4,6));
    String regex = buildRegexFromInt(year,month,id);
    return regex;
  }

  private static String buildRegexFromInt(int year, int month,long id){
    String s = ".*(";

    s += "(" + getYearRange(year-30,year-1) + getMonthRange(1,12) + "|";
    s += year + getMonthRange(1,month) + ")";

    s += "-";

    s += "(" + getYearRange(year+1,year+30) + getMonthRange(1,12) + "|";
    s += year + getMonthRange(month,12) + ")";

    s += ")-" + id + ".*";
    return s;
  }
  private static String getYearRange(int a, int b){
    String s = "(" + a/10 + "[" + a%10 + "-9]";
    a = a/10 + 1;
    for(int i = a; i<b/10; i+= 1){
      s += "|" + a + "[0-9]";
    }
    s += "|" + b/10 + "[0-" + b%10 + "])";
    return s;
  }
  private static String getMonthRange(int a, int b){
    String s = "(";
    if(a>=10){
      s+= "1[" + a%10 + "-" + b%10 + "]";
    }
    else if(a<10 && b >=10){
      s+= "0[" + a%10 + "-9]|";
      s+= "1[0-" + b%10 + "]";
    }
    else{
      s+= "0[" + a + "-" + b + "]";
    }
    s+= ")";
    return s;
  }

}
