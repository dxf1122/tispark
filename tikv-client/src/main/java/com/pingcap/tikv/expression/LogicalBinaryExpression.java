// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: expression.proto

package com.pingcap.tikv.expression;


import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

public class LogicalBinaryExpression implements Expression {
  public enum Type {
    AND,
    OR,
    XOR
  }

  public static LogicalBinaryExpression and(Expression left, Expression right) {
    return new LogicalBinaryExpression(Type.AND, left, right);
  }

  public static LogicalBinaryExpression or(Expression left, Expression right) {
    return new LogicalBinaryExpression(Type.OR, left, right);
  }

  public static LogicalBinaryExpression xor(Expression left, Expression right) {
    return new LogicalBinaryExpression(Type.XOR, left, right);
  }

  public LogicalBinaryExpression(Type type, Expression left, Expression right) {
    this.left = requireNonNull(left, "left expression is null");
    this.right = requireNonNull(right, "right expression is null");
    this.compType = requireNonNull(type, "type is null");
  }

  @Override
  public List<Expression> getChildren() {
    return null;
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  public Type getCompType() {
    return compType;
  }

  private final Expression left;
  private final Expression right;
  private final Type compType;

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    LogicalBinaryExpression that = (LogicalBinaryExpression) other;
    return (compType == that.getCompType()) &&
        left.equals(that.left) &&
        right.equals(that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compType, left, right);
  }

  @Override
  public String toString() {
    return String.format("%s %s %s", getLeft(), getCompType(), getRight());
  }
}
