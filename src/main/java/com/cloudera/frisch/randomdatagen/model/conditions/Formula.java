package com.cloudera.frisch.randomdatagen.model.conditions;

import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.LinkedList;


public class Formula {

  private static final Logger logger = Logger.getLogger(Formula.class);

  // for all cols name existing in model, try to find which one are involved in the formula and put them in a list
  @Getter @Setter
  private LinkedList<String> listOfColsToEvaluate;

  @Getter @Setter
  private String formulaToEvaluate;

  private final ScriptEngineManager scriptEngineManager;
  private final ScriptEngine scriptEngine;

  Formula(String formula) {
    // fill in the listOfColsToEvaluate + Create formula string with no $
    listOfColsToEvaluate = new LinkedList<>();
    for(String field: formula.substring(formula.indexOf("$")+1).split("[$]")) {
        listOfColsToEvaluate.add(field.split("\\s+")[0]);
        logger.debug("Add Field : " + field.split("\\s+")[0] + " to be in the formula");
    }
    formulaToEvaluate = formula.replaceAll("[$]", "");
    scriptEngineManager = new ScriptEngineManager();
    scriptEngine = scriptEngineManager.getEngineByName("JavaScript");
  }

  public String evaluateFormula(Row row) {
    // Evaluate formula using an evaluator (or built this evaluator)
    String formulaReplaced = formulaToEvaluate;
    for(String colName: listOfColsToEvaluate) {
      logger.debug(formulaReplaced);
      formulaReplaced = formulaReplaced.replaceAll("(^| )" + colName + "($| )", row.getValues().get(colName).toString());
    }
    logger.debug(formulaReplaced);
    return computeFormula(formulaReplaced);
  }

  private String computeFormula(String formula) {
    Object value = 0f;
    try {
       value = scriptEngine.eval(formula);
       logger.debug("Evaluating formula: " + formula + " to: " + value);
    } catch (ScriptException e) {
      logger.warn("Could not evaluate expression: " + formula + " due to error: ", e);
    }
    return value.toString();
  }



}
