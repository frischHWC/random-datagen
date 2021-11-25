package com.cloudera.frisch.randomdatagen.model.conditions;


import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import com.cloudera.frisch.randomdatagen.model.type.Field;
import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.util.*;

public class ConditionalEvaluator {

  Logger logger = Logger.getLogger(ConditionalEvaluator.class);

  @Getter
  @Setter
  public Map<String, LinkedList<ConditionsLine>> conditions;


  @Getter
  @Setter
  public Map<String, String> mapOfFieldTypes;

  // TODO: Refactor project to be simpler and more efficient by using a Map of key=column_name to value=value on the column


  public ConditionalEvaluator(Model model) {
    conditions = new HashMap<>();

    LinkedList<Field> fields =  (LinkedList<Field>) model.getFields();

    for(Field field: fields) {
      if(field.conditionals!= null && !field.conditionals.isEmpty()) {
        logger.info("Field has been marked as conditional: " + field);

        LinkedList<ConditionsLine> condLineList = new LinkedList<>();
        Iterator<Map.Entry> condIterator = field.conditionals.entrySet().iterator();
        while(condIterator.hasNext()) {
          Map.Entry<String, String> condLine = condIterator.next();
          condLineList.add(new ConditionsLine(condLine.getKey(), condLine.getValue(), model));
        }
        conditions.put(field.name, condLineList);
      }

    }
  }


  // Use this evaluator to satisfy conditions before adding it to rows list
  public Row evaluateConditions(Row row, Model model) {
    if(!conditions.isEmpty()) {
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
