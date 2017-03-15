package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.trees.TreeNodeRef
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.CarbonPushDownToScan
import org.apache.carbondata.core.scan.model.QueryDimension

/**
 * A pattern that matches any number of project or filter operations on top of another relational
 * operator.  All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator.
 * [[org.apache.spark.sql.catalyst.expressions.Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
object PhysicalOperationForPushdown extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan, Seq[QueryDimension], Int, Seq[Expression], Seq[NamedExpression])

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _, sorts, limitValue, grpExp, aggExp) = collectSortsAndProjectsAndFilters(plan)
    Some((fields.getOrElse(child.output), filters, child, sorts, limitValue, grpExp, aggExp))
  }

  /**
   * Collects projects and filters, in-lining/substituting aliases if necessary.  Here are two
   * examples for alias in-lining/substitution.  Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
   */
  def collectSortsAndProjectsAndFilters(plan: LogicalPlan): (Option[Seq[NamedExpression]],
      Seq[Expression], LogicalPlan, Map[Attribute, Expression],
      Seq[QueryDimension], Int, Seq[Expression], Seq[NamedExpression]) =
    plan match {

      case CarbonPushDownToScan(order, limit, groupingExpressions, aggregateExpressions, child) =>
        val (fields, filters, other, aliases,
            _, _, _, _) = collectSortsAndProjectsAndFilters(child)
        //var findSorts = order
        var limitVal: Int = 0;
        if (limit != null) {
          limitVal = limit.asInstanceOf[Literal].value.asInstanceOf[Int]
        }
        (fields, filters, other, aliases, order,
            limitVal, groupingExpressions, aggregateExpressions)

      case Project(fields, child) =>
        val (_, filters, other, aliases, sorts, limitValue, groupingExpressions,
            aggregateExpressions) = collectSortsAndProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields),
            sorts, limitValue, groupingExpressions, aggregateExpressions)

      case Filter(condition, child) =>
        val (fields, filters, other, aliases, sorts, limitValue,
            groupingExpressions, aggregateExpressions) = collectSortsAndProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition),
            other, aliases, sorts, limitValue, groupingExpressions, aggregateExpressions)

      case other =>
        (None, Nil, other, Map.empty, Nil, 0, Nil, Nil)
    }

  def collectAliases(fields: Seq[Expression]): Map[Attribute, Expression] = fields.collect {
    case a @ Alias(child, _) => a.toAttribute -> child
  }.toMap

  def substitute(aliases: Map[Attribute, Expression])(expr: Expression): Expression = {
    expr.transform {
      case a @ Alias(ref: AttributeReference, name) =>
        aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
    }
  }
}