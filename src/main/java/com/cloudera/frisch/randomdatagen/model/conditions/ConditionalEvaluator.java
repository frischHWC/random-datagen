package com.cloudera.frisch.randomdatagen.model.conditions;


import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.util.*;

public class ConditionalEvaluator {

  private final static Logger logger = Logger.getLogger(ConditionalEvaluator.class);

  @Getter
  @Setter
  public LinkedList<ConditionsLine> conditions = new LinkedList<>();

  /*
  A conditionalEvaluator is responsible for preparing conditions evaluations set on a field by parsing the list of conditions to met
   */
  public ConditionalEvaluator(LinkedHashMap<String, String> conditionals) {
        Iterator<Map.Entry<String, String>> condIterator = conditionals.entrySet().iterator();
        while(condIterator.hasNext()) {
          Map.Entry<String, String> condLine = condIterator.next();
          conditions.add(new ConditionsLine(condLine.getKey(), condLine.getValue()));
          logger.debug(" Added condition line: " + condLine.getKey() + " : " + condLine.getValue());
        }
  }

  public String evaluateConditions(Row row) {
    for(ConditionsLine cl: conditions){
      if(cl.isLineSatisfied(row)) {
        return cl.getValueToReturn();
      }
    }
    return "";
  }


}
