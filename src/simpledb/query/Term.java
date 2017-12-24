package simpledb.query;

import simpledb.record.Schema;

/**
 * A term is a comparison between two expressions.
 * @author Edward Sciore
 *
 */
public class Term {
   private Expression lhs, rhs;
   private int rlat;
   /*
    * rlat=0 : !=
    * rlat=1 : =
    * rlat=2 : >
    * rlat=3 : <
    */
   
   /**
    * Creates a new term that compares two expressions
    * for equality.
    * @param lhs  the LHS expression
    * @param rhs  the RHS expression
    */
   public Term(Expression lhs, Expression rhs, int rlat) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.rlat = rlat;
   }
   
   /**
    * Calculates the extent to which selecting on the term reduces 
    * the number of records output by a query.
    * For example if the reduction factor is 2, then the
    * term cuts the size of the output in half.
    * @param p the query's plan
    * @return the integer reduction factor.
    */
   public int reductionFactor(Plan p) {
      String lhsName, rhsName;
      if (lhs.isFieldName() && rhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         rhsName = rhs.asFieldName();
         return Math.max(p.distinctValues(lhsName),
                         p.distinctValues(rhsName));
      }
      if (lhs.isFieldName()) {
         lhsName = lhs.asFieldName();
         return p.distinctValues(lhsName);
      }
      if (rhs.isFieldName()) {
         rhsName = rhs.asFieldName();
         return p.distinctValues(rhsName);
      }
      // otherwise, the term equates constants
      boolean flag = false;
      switch(rlat){
         case 0:
            if (! lhs.asConstant().equals(rhs.asConstant())) flag = true;
            break;
         case 1:
            if (lhs.asConstant().equals(rhs.asConstant())) flag = true;
            break;
         case 2:
            //if (lhs.asConstant() instanceof String && rhs.asConstant() instanceof String && )
            if (lhs.asConstant().compareTo(rhs.asConstant()) > 0) flag = true;
            break;
         case 3:
            //if (lhs.asConstant() instanceof String && rhs.asConstant() instanceof String && )
            if (lhs.asConstant().compareTo(rhs.asConstant()) < 0) flag = true;
            break;
         default:
            break;
      }
      if (flag)
         return 1;
      else
         return Integer.MAX_VALUE;
   }
   
   /**
    * Determines if this term is of the form "F=c"
    * where F is the specified field and c is some constant.
    * If so, the method returns that constant.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the constant or null
    */
   public Constant equatesWithConstant(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          rhs.isConstant())
         return rhs.asConstant();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               lhs.isConstant())
         return lhs.asConstant();
      else
         return null;
   }
   
   /**
    * Determines if this term is of the form "F1=F2"
    * where F1 is the specified field and F2 is another field.
    * If so, the method returns the name of that field.
    * If not, the method returns null.
    * @param fldname the name of the field
    * @return either the name of the other field, or null
    */
   public String equatesWithField(String fldname) {
      if (lhs.isFieldName() &&
          lhs.asFieldName().equals(fldname) &&
          rhs.isFieldName())
         return rhs.asFieldName();
      else if (rhs.isFieldName() &&
               rhs.asFieldName().equals(fldname) &&
               lhs.isFieldName())
         return lhs.asFieldName();
      else
         return null;
   }
   
   /**
    * Returns true if both of the term's expressions
    * apply to the specified schema.
    * @param sch the schema
    * @return true if both expressions apply to the schema
    */
   public boolean appliesTo(Schema sch) {
      return lhs.appliesTo(sch) && rhs.appliesTo(sch);
   }
   
   /**
    * Returns true if both of the term's expressions
    * evaluate to the same constant,
    * with respect to the specified scan.
    * @param s the scan
    * @return true if both expressions have the same value in the scan
    */
   public boolean isSatisfied(Scan s) {
      Constant lhsval = lhs.evaluate(s);
      Constant rhsval = rhs.evaluate(s);
      boolean flag = false;
      switch(rlat){
         case 0: if(!lhsval.equals(rhsval)) flag = true; break;
         case 1: if(lhsval.equals(rhsval)) flag = true; break;
         case 2: if(lhsval.compareTo(rhsval) > 0) flag = true; break;
         case 3: if(lhsval.compareTo(rhsval) < 0) flag = true; break;
         default: break;
      }
      return flag;
   }
   
   public String toString() {
      String rlat_str;
      switch(rlat){
         case 0: rlat_str = "!="; break;
         case 1: rlat_str = "="; break;
         case 2: rlat_str = ">"; break;
         case 3: rlat_str = "<"; break;
         default: rlat_str = "=";
      }
      return lhs.toString() + "=" + rhs.toString();
   }
}
