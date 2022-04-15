package com.cloudera.frisch.randomdatagen.model.conditions;


import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.model.type.Field;
import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.util.*;

public class ConditionalEvaluator {

  private final static Logger logger = Logger.getLogger(ConditionalEvaluator.class);

  @Getter
  @Setter
  public LinkedList<ConditionsLine> conditions;

  // TODO: Refactor project to be simpler and more efficient by using a Map of key=column_name to value=value on the column
  // Make one Conditional Evaluator linked to a Field, so we can be able within a field to compute its value based on confitions created


  public ConditionalEvaluator(Field field) {

      if(field.conditionals!= null && !field.conditionals.isEmpty()) {
        logger.debug("Field has been marked as conditional: " + field);

        LinkedList<ConditionsLine> condLineList = new LinkedList<>();
        Iterator<Map.Entry> condIterator = field.conditionals.entrySet().iterator();
        while(condIterator.hasNext()) {
          Map.Entry<String, String> condLine = condIterator.next();
          condLineList.add(new ConditionsLine(condLine.getKey(), condLine.getValue(), model));
          logger.debug("For field: " + field.getName() + " added condition line: " + condLine.getKey() + " : " + condLine.getValue());
        }
        conditions.put(field.name, condLineList);
      }

  }


  // Use this evaluator to satisfy conditions before adding it to rows list

  /**
   * Evaluate all conditions on a row
   * @param row
   * @param model
   * @return
   */
  public Row evaluateConditions(Row row, Model model) {
    if(!conditions.isEmpty()) {
      logger.debug("Will evaluate conditions as Row: " + row.toString() + " has some");
      Iterator<Map.Entry<String, LinkedList<ConditionsLine>>> conditionIterator = conditions.entrySet().iterator();

      while (conditionIterator.hasNext()){
        Map.Entry<String, LinkedList<ConditionsLine>> conditionsOnaField = conditionIterator.next();

        for(ConditionsLine cl : conditionsOnaField.getValue()) {
          if(cl.isLineSatisfied(row, model)){
            row.getValues().put(model.findFieldasFieldWhoseNameIs(conditionsOnaField.getKey()), cl.getValueToReturn());
          }
        }

      }

    }
    return row;
  }


}
