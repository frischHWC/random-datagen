package com.cloudera.frisch.randomdatagen.model.conditions;

import com.cloudera.frisch.randomdatagen.model.Model;
import com.cloudera.frisch.randomdatagen.model.Row;
import lombok.Getter;
import lombok.Setter;
import org.apache.log4j.Logger;

public class Condition {

  private static final Logger logger = Logger.getLogger(Condition.class);

  @Getter @Setter
  String columnName1;

  @Getter @Setter
  String columnType1;

  @Getter @Setter
  String columnName2;

  @Getter @Setter
  String columnType2;

  @Getter @Setter
  String value2;

  @Getter @Setter
  Integer value2AsInt;

  @Getter @Setter
  Long value2AsLong;

  @Getter @Setter
  Float value2AsFloat;

  @Getter @Setter
  Operators operator;


  Condition(String columnName1, String columnName2, String value2, String operator, Model model) {
    this.columnName1 = columnName1;
    this.columnName2 = columnName2;
    this.value2 = value2;
    switch(operator) {
      case "=": this.operator = Operators.EQUALS;
                break;
      case "!": this.operator = Operators.UNEQUALS;
                break;
      case ">": this.operator = Operators.SUPERIOR;
                break;
      case "<": this.operator = Operators.INFERIOR;
                break;
    }

    // Check if values are comparable
    this.columnType1 = model.findFieldasFieldWhoseNameIs(columnName1).getClass().getSimpleName();
    if(columnName2!=null) {
      this.columnType2 =
          model.findFieldasFieldWhoseNameIs(columnName2).getClass()
              .getSimpleName();
      if (!this.columnType1.equalsIgnoreCase(
          model.findFieldasFieldWhoseNameIs(columnName1).getClass()
              .getSimpleName())) {
        logger.error("Could not compare columns with not the same type: col: " +
            columnName1 + " is " + this.columnType1 + " and " +
            columnName2 + " is " + this.columnType2);
        System.exit(1);
      } else {
        logger.debug("Comparison will be made between " + columnName1 + " and " +
            columnName2 + " which are both " + this.columnType1);
      }
    }

    if(value2 != null) {
      try {
        switch (this.columnType1) {
        case "LongField":
          value2AsLong = Long.valueOf(value2);
          break;
        case "IntegerField":
          value2AsInt = Integer.valueOf(value2);
          break;
        case "Floatfield":
          value2AsFloat = Float.valueOf(value2);
          break;
        default: break;
        }
      } catch (ClassCastException e) {
        logger.error("Could not cast value of comparison to the right type: " + this.columnType1, e);
        System.exit(1);
      }

      logger.debug("Comparison will be made between " + columnName1 + " and " + value2 + " which are both " + this.columnType1);
    }

  }


  public boolean evaluateCondition(Row row, Model model) {
    boolean result;
    String firstValue = row.getValues().get(model.findFieldasFieldWhoseNameIs(columnName1)).toString();
    String secondValue = columnName2 == null ? value2.trim() :
        row.getValues().get(model.findFieldasFieldWhoseNameIs(columnName2)).toString().trim();

    switch (this.operator) {
    case EQUALS:
      result = firstValue.equalsIgnoreCase(secondValue);
      break;

    case UNEQUALS:
      result = !firstValue.equalsIgnoreCase(secondValue);
      break;

    case INFERIOR:
      result = !isSuperior(firstValue, secondValue);
      break;

    case SUPERIOR:
      result = isSuperior(firstValue, secondValue);
      break;

    default:
      result = false;
      logger.warn("Could not get any operator to evaluate condition");
      break;
    }

    return result;
  }

  private boolean isSuperior(String firstValue, String secondValue) {
    switch (this.columnType1) {
    case "LongField":
      return value2AsLong == null ?  Long.parseLong(firstValue) > Long.parseLong(secondValue) : Long.parseLong(firstValue) > value2AsLong;
    case "IntegerField":
      return value2AsInt == null ?  Integer.parseInt(firstValue) > Integer.parseInt(secondValue) : Long.parseLong(firstValue) > value2AsInt;
    case "Floatfield":
      return value2AsFloat == null ?  Float.parseFloat(firstValue) > Float.parseFloat(secondValue) : Float.parseFloat(firstValue) > value2AsFloat;
    default:
      return firstValue.compareTo(secondValue) > 0;
    }
  }






}
