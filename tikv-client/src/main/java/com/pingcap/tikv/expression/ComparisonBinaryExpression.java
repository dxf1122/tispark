/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.expression;


import static com.google.common.base.Preconditions.checkArgument;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.Type.EQUAL;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.Type.GREATER_EQUAL;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.Type.GREATER_THAN;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.Type.LESS_EQUAL;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.Type.LESS_THAN;
import static com.pingcap.tikv.expression.ComparisonBinaryExpression.Type.NOT_EQUAL;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.key.TypedKey;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class ComparisonBinaryExpression implements Expression {
  public enum Type {
    EQUAL,
    NOT_EQUAL,
    LESS_THAN,
    LESS_EQUAL,
    GREATER_THAN,
    GREATER_EQUAL
  }

  public static ComparisonBinaryExpression equal(Expression left, Expression right) {
    return new ComparisonBinaryExpression(EQUAL, left, right);
  }

  public static ComparisonBinaryExpression notEqual(Expression left, Expression right) {
    return new ComparisonBinaryExpression(NOT_EQUAL, left, right);
  }

  public static ComparisonBinaryExpression lessThan(Expression left, Expression right) {
    return new ComparisonBinaryExpression(LESS_THAN, left, right);
  }

  public static ComparisonBinaryExpression lessEqual(Expression left, Expression right) {
    return new ComparisonBinaryExpression(LESS_EQUAL, left, right);
  }

  public static ComparisonBinaryExpression greaterThan(Expression left, Expression right) {
    return new ComparisonBinaryExpression(GREATER_THAN, left, right);
  }

  public static ComparisonBinaryExpression greaterEqual(Expression left, Expression right) {
    return new ComparisonBinaryExpression(GREATER_EQUAL, left, right);
  }

  public static class NormalizedPredicate {
    private final ComparisonBinaryExpression pred;
    private TypedKey key;

    NormalizedPredicate(ComparisonBinaryExpression pred) {
      checkArgument(pred.getLeft() instanceof ColumnRef);
      checkArgument(pred.getRight() instanceof Constant);
      this.pred = pred;
    }

    public ColumnRef getColumnRef() {
      return (ColumnRef) pred.getLeft();
    }

    public Constant getValue() {
      return (Constant) pred.getRight();
    }

    public Type getType() {
      return pred.getComparisonType();
    }

    public TypedKey getTypedLiteral() {
      if (key == null) {
        key = TypedKey.toTypedKey(getValue().getValue(), getColumnRef().getType());
      }
      return key;
    }
  }

  private static final Map<Type, ExprType> typeMap = ImmutableMap.<Type, ExprType>builder()
      .put(EQUAL, ExprType.EQ)
      .put(Type.NOT_EQUAL, ExprType.NE)
      .put(Type.LESS_THAN, ExprType.LT)
      .put(Type.LESS_EQUAL, ExprType.LE)
      .put(Type.GREATER_THAN, ExprType.LT)
      .put(Type.GREATER_EQUAL, ExprType.GE)
      .build();

  private final Expression left;
  private final Expression right;
  private final Type compType;
  private Optional<NormalizedPredicate> normalizedPredicate;

  public ComparisonBinaryExpression(Type type, Expression left, Expression right) {
    this.left = requireNonNull(left, "left expression is null");
    this.right = requireNonNull(right, "right expression is null");
    this.compType = requireNonNull(type, "type is null");
  }

  @Override
  public List<Expression> getChildren() {
    return ImmutableList.of(left, right);
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

  public Type getComparisonType() {
    return compType;
  }

  public NormalizedPredicate normalize() {
    if (normalizedPredicate != null) {
      return normalizedPredicate.orElseGet(() -> null);
    }
    if (getLeft() instanceof Constant && getRight() instanceof ColumnRef) {
      Constant left = (Constant) getLeft();
      ColumnRef right = (ColumnRef) getRight();
      Type newType;
      switch (getComparisonType()) {
        case EQUAL:
          newType = EQUAL;
          break;
        case LESS_EQUAL:
          newType = Type.GREATER_EQUAL;
          break;
        case LESS_THAN:
          newType = Type.GREATER_THAN;
          break;
        case GREATER_EQUAL:
          newType = Type.LESS_EQUAL;
          break;
        case GREATER_THAN:
          newType = Type.LESS_THAN;
          break;
        case NOT_EQUAL:
          newType = NOT_EQUAL;
          break;
        default:
          throw new TiExpressionException(
              String.format("PredicateNormalizer is not able to process type %s", getComparisonType())
          );
      }
      ComparisonBinaryExpression newExpression = new ComparisonBinaryExpression(newType, right, left);
      normalizedPredicate = Optional.of(new NormalizedPredicate(newExpression));
      return normalizedPredicate.get();
    } else if (getRight() instanceof Constant && getLeft() instanceof ColumnRef) {
      normalizedPredicate = Optional.of(new NormalizedPredicate(this));
      return normalizedPredicate.get();
    } else {
      return null;
    }
  }

  @Override
  public String toString() {
    return String.format("[%s %s %s]", getLeft(), getComparisonType(), getRight());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    ComparisonBinaryExpression that = (ComparisonBinaryExpression) other;
    return (compType == that.compType) &&
        left.equals(that.left) &&
        right.equals(that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compType, left, right);
  }
}
