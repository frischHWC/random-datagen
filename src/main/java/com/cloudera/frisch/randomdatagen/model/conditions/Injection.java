package com.cloudera.frisch.randomdatagen.model.conditions;


import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.AllArgsConstructor;
import org.apache.log4j.Logger;


import java.util.LinkedList;
import java.util.Map;


public class Injection {

  @AllArgsConstructor
  private class InjectedField {
    String stringToPrint;
    Boolean toReplace;
  }

  private static final Logger logger = Logger.getLogger(Injection.class);

  private final LinkedList<InjectedField> injectedFieldNames = new LinkedList<>();

  Injection(String injection) {
    for(String s: injection.split("[$]")) {
      if (s.charAt(0)!='{') {
        logger.debug(s + " is not a variable name");
        injectedFieldNames.add(new InjectedField(s, false));
      } else {
        String fieldToAdd = s.substring(1,s.indexOf('}'));
        logger.debug(fieldToAdd + " is found as a variable name");
        injectedFieldNames.add(new InjectedField(fieldToAdd, true));
        if(s.length()>s.indexOf('}')){
          logger.debug(s.substring(s.indexOf('}')+1) + " is not a variable name");
          injectedFieldNames.add(new InjectedField(s.substring(s.indexOf('}')+1), false));
        }
      }
    }
  }

  public String evaluateInjection(Row row) {
    Map<String, Object> rowValues = row.getValues();
    StringBuilder sb = new StringBuilder();
    try {
      for(InjectedField fieldNameToReplace: injectedFieldNames) {
        if(fieldNameToReplace.toReplace) {
          sb.append(rowValues.get(fieldNameToReplace.stringToPrint).toString());
        } else {
         sb.append(fieldNameToReplace.stringToPrint);
        }
      }
    } catch (Exception e) {
      logger.error("Can not evaluate injection so returning empty value, see: ", e);
    }

    return sb.toString();
  }


}
