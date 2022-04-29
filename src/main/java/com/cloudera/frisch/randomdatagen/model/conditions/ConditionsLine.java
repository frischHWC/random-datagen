package com.cloudera.frisch.randomdatagen.model.conditions;

import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


/**
 * A Conditions line is a line of 1 or multiple conditions evaluated in order
 */
public class ConditionsLine {

  Logger logger = Logger.getLogger(ConditionsLine.class);

  @Getter @Setter
  private LinkedList<Condition> listOfConditions;

  @Getter @Setter
  private LinkedList<ConditionOperators> listOfConditionsOperators;


  // To indicate if there are multiple conditions on this line or only one
  @Getter @Setter
  private boolean combinedCondition = false;

  @Getter @Setter
  private boolean formula = false;

  @Getter @Setter
  private Formula formulaToEvaluate;

  @Getter @Setter
  private boolean defaultValue = false;

  @Getter @Setter
  private String valueToReturn;

  @Getter @Setter
  private boolean link = false;

  @Getter @Setter
  private Link linkToEvaluate;



  public ConditionsLine(String conditionLine, String valueToReturn) {
    this.valueToReturn = valueToReturn;
    this.listOfConditionsOperators = new LinkedList<>();
    this.listOfConditions = new LinkedList<>();

    // 1st: break using space => That will isolate if there are multiple parts
    String[] conditionSplitted = conditionLine.trim().split(" ");

    if(conditionSplitted.length>1){
      logger.debug("Found a combined condition on this line");
      this.combinedCondition=true;
    } else if(conditionSplitted[0].equalsIgnoreCase("always")) {
      logger.debug("Found a formula, that will need to be evaluated");
      this.formula = true;
      this.formulaToEvaluate = new Formula(valueToReturn);
      return;
    } else if(conditionSplitted[0].equalsIgnoreCase("link")) {
      logger.debug("Found a link, that will need to be evaluated");
      this.link = true;
      this.linkToEvaluate = new Link(valueToReturn);
      return;
    } else if(conditionSplitted[0].equalsIgnoreCase("default")) {
      logger.debug("Found a default, No evaluation needed");
      this.defaultValue = true;
      return;
    }

    int index = 0;
    for(String s: conditionSplitted){
      if(index%2==0) {
        logger.debug("This is an expression that will create a condition");
        listOfConditions.add(createConditionFromExpression(s));
      } else {
        logger.debug("This is an expression that will create an operator between conditions");
        listOfConditionsOperators.add(createOperatorFromExpression(s));
      }
      index++;
    }

  }

  private ConditionOperators createOperatorFromExpression(String operatorExpression) {
    if(operatorExpression.trim().equalsIgnoreCase("|")) {
      return ConditionOperators.OR;
    } else {
      return ConditionOperators.AND;
    }
  }

  private Condition createConditionFromExpression(String conditionExpression) {
    String[] conditionVals = null;
    String operator = "=";
    if (conditionExpression.contains("=")) {
      conditionVals = conditionExpression.trim().split("=");
    } else if (conditionExpression.contains("!")) {
      conditionVals = conditionExpression.trim().split("!");
      operator = "!";
    } else if (conditionExpression.contains("<")) {
      conditionVals = conditionExpression.trim().split("<");
      operator = "<";
    } else if (conditionExpression.contains(">")) {
      conditionVals = conditionExpression.trim().split(">");
      operator = ">";
    }

    if (conditionVals != null) {
      if (conditionVals[1].matches("[$].*")) {
        logger.debug("2nd option is a column name, not a value");
        return new Condition(conditionVals[0].substring(1),
            conditionVals[1].substring(1), null, operator);
      } else {
        return new Condition(conditionVals[0].substring(1), null,
            conditionVals[1], operator);
      }
    }

    return null;
  }

  public boolean isLineSatisfied(Row row) {
    if(!combinedCondition) {
      if(!listOfConditions.isEmpty()) {
        return listOfConditions.get(0).evaluateCondition(row);
      } else if(this.formula) {
        // Formula case
        this.valueToReturn = formulaToEvaluate.evaluateFormula(row);
        return true;
      } else if(this.link) {
        // Formula case
        this.valueToReturn = linkToEvaluate.evaluateLink(row);
        return true;
      } else {
        // Default case
        return true;
      }

    } else {
      // To evaluate a condition assuming AND has precedence over OR, we should:
      // 1. Isolate groups of AND
      // 2. Evaluate each AND group and return true if one is true, false else
      List<Boolean> conditionsGroupEvaluationResult = new ArrayList<>();
      boolean previousResult = listOfConditions.get(0).evaluateCondition(row);
      for(int i = 1; i<listOfConditions.size(); i++) {

        if(listOfConditionsOperators.get(i-1)==ConditionOperators.AND) {
          logger.debug("The operator between previous condition and this one is AND");
          if(previousResult){
            logger.debug("Previous condition was true, need to evaluate this one");
            previousResult = listOfConditions.get(i).evaluateCondition(row);
          } else {
            logger.debug("Previous condition was false, no need to evaluate this one");
          }
        } else {
          logger.debug("The operator between previous condition and this one is OR, so keep previous result and start to evaluate new condition");
          conditionsGroupEvaluationResult.add(previousResult);
          previousResult = listOfConditions.get(i).evaluateCondition(row);
        }
      }
      return conditionsGroupEvaluationResult.contains(true);
    }

  }

}
