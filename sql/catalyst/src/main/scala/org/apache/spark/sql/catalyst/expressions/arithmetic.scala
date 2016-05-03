/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.types._

case class UnaryMinus(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  override def dataType: DataType = child.dataType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def toString: String = s"-$child"

  private[this] val numeric =
    if (resolved) {
      dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case other => UnresolvedNumeric.asInstanceOf[Numeric[Any]]
      }
    } else {
      UnresolvedNumeric.asInstanceOf[Numeric[Any]]
    }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      numeric.negate(evalE)
    }
  }
}

case class Sqrt(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  override def dataType: DataType = DoubleType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = true
  override def toString: String = s"SQRT($child)"

  private[this] val numeric =
    if (resolved) {
      child.dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case other => UnresolvedNumeric.asInstanceOf[Numeric[Any]]
      }
    } else {
      UnresolvedNumeric.asInstanceOf[Numeric[Any]]
    }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val value = numeric.toDouble(evalE)
      if (value < 0) null
      else math.sqrt(value)
    }
  }
}

abstract class BinaryArithmetic extends BinaryExpression {
  self: Product =>

  type EvaluatedType = Any

  override lazy val resolved =
    left.resolved && right.resolved &&
    left.dataType == right.dataType &&
    !DecimalType.isFixed(left.dataType)

  override def dataType: DataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        evalInternal(evalE1, evalE2)
      }
    }
  }

  def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any =
    sys.error(s"BinaryExpressions must either override eval or evalInternal")
}

case class Add(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "+"

  private[this] val numeric =
    if (resolved) {
      dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case other => UnresolvedNumeric.asInstanceOf[Numeric[Any]]
      }
    } else {
      UnresolvedNumeric.asInstanceOf[Numeric[Any]]
    }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        numeric.plus(evalE1, evalE2)
      }
    }
  }
}

case class Subtract(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "-"

  private[this] val numeric =
    if (resolved) {
      dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case other => UnresolvedNumeric.asInstanceOf[Numeric[Any]]
      }
    } else {
      UnresolvedNumeric.asInstanceOf[Numeric[Any]]
    }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        numeric.minus(evalE1, evalE2)
      }
    }
  }
}

case class Multiply(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "*"

  private[this] val numeric =
    if (resolved) {
      dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case other => UnresolvedNumeric.asInstanceOf[Numeric[Any]]
      }
    } else {
      UnresolvedNumeric.asInstanceOf[Numeric[Any]]
    }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        numeric.times(evalE1, evalE2)
      }
    }
  }
}

case class Divide(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "/"

  override def nullable: Boolean = true

  private[this] val div: (Any, Any) => Any =
    if (resolved) {
      dataType match {
        case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
        case it: IntegralType => it.integral.asInstanceOf[Integral[Any]].quot
        case other => UnresolvedNumeric.asInstanceOf[Integral[Any]].quot
      }
    } else {
      UnresolvedNumeric.asInstanceOf[Integral[Any]].quot
    }
  
  override def eval(input: Row): Any = {
    val evalE2 = right.eval(input)
    if (evalE2 == null || evalE2 == 0) {
      null
    } else {
      val evalE1 = left.eval(input)
      if (evalE1 == null) {
        null
      } else {
        div(evalE1, evalE2)
      }
    }
  }
}

case class Remainder(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "%"

  override def nullable: Boolean = true

  private[this] val integral =
    if (resolved) {
      dataType match {
        case i: IntegralType => i.integral.asInstanceOf[Integral[Any]]
        case i: FractionalType => i.asIntegral.asInstanceOf[Integral[Any]]
        case other => UnresolvedNumeric.asInstanceOf[Integral[Any]]
      }
    } else {
      UnresolvedNumeric.asInstanceOf[Integral[Any]]
    }

  override def eval(input: Row): Any = {
    val evalE2 = right.eval(input)
    if (evalE2 == null || evalE2 == 0) {
      null
    } else {
      val evalE1 = left.eval(input)
      if (evalE1 == null) {
        null
      } else {
        integral.rem(evalE1, evalE2)
      }
    }
  }
}

/**
 * A function that calculates bitwise and(&) of two numbers.
 */
case class BitwiseAnd(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "&"

  private[this] val and: (Any, Any) => Any =
    if (resolved) {
      dataType match {
        case ByteType =>
          ((evalE1: Byte, evalE2: Byte) =>
            (evalE1 & evalE2).toByte).asInstanceOf[(Any, Any) => Any]
        case ShortType =>
          ((evalE1: Short, evalE2: Short) =>
            (evalE1 & evalE2).toShort).asInstanceOf[(Any, Any) => Any]
        case IntegerType =>
          ((evalE1: Int, evalE2: Int) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
        case LongType =>
          ((evalE1: Long, evalE2: Long) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
        case other => UnresolvedNumeric.bitwiseAnd
      }
    } else {
      UnresolvedNumeric.bitwiseAnd
    }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any =
    and(evalE1, evalE2)
}

/**
 * A function that calculates bitwise or(|) of two numbers.
 */
case class BitwiseOr(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "|"

  private[this] val or: (Any, Any) => Any =
    if (resolved) {
      dataType match {
        case ByteType =>
          ((evalE1: Byte, evalE2: Byte) =>
            (evalE1 | evalE2).toByte).asInstanceOf[(Any, Any) => Any]
        case ShortType =>
          ((evalE1: Short, evalE2: Short) =>
            (evalE1 | evalE2).toShort).asInstanceOf[(Any, Any) => Any]
        case IntegerType =>
          ((evalE1: Int, evalE2: Int) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
        case LongType =>
          ((evalE1: Long, evalE2: Long) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
        case other => UnresolvedNumeric.bitwiseOr
      }
    } else {
      UnresolvedNumeric.bitwiseOr
    }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = or(evalE1, evalE2)
}

/**
 * A function that calculates bitwise xor(^) of two numbers.
 */
case class BitwiseXor(left: Expression, right: Expression) extends BinaryArithmetic {
  override def symbol: String = "^"

  private[this] val xor: (Any, Any) => Any =
    if (resolved) {
      dataType match {
        case ByteType =>
          ((evalE1: Byte, evalE2: Byte) =>
            (evalE1 ^ evalE2).toByte).asInstanceOf[(Any, Any) => Any]
        case ShortType =>
          ((evalE1: Short, evalE2: Short) =>
            (evalE1 ^ evalE2).toShort).asInstanceOf[(Any, Any) => Any]
        case IntegerType =>
          ((evalE1: Int, evalE2: Int) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
        case LongType =>
          ((evalE1: Long, evalE2: Long) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
        case other => UnresolvedNumeric.bitwiseXor
      }
    } else {
      UnresolvedNumeric.bitwiseXor
    }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = xor(evalE1, evalE2)
}

/**
 * A function that calculates bitwise not(~) of a number.
 */
case class BitwiseNot(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  override def dataType: DataType = child.dataType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def toString: String = s"~$child"

  private[this] val not: (Any) => Any =
    if (resolved) {
      dataType match {
        case ByteType =>
          ((evalE: Byte) => (~evalE).toByte).asInstanceOf[(Any) => Any]
        case ShortType =>
          ((evalE: Short) => (~evalE).toShort).asInstanceOf[(Any) => Any]
        case IntegerType =>
          ((evalE: Int) => ~evalE).asInstanceOf[(Any) => Any]
        case LongType =>
          ((evalE: Long) => ~evalE).asInstanceOf[(Any) => Any]
        case other => UnresolvedNumeric.bitwiseNot
      }
    } else {
      UnresolvedNumeric.bitwiseNot
    }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      not(evalE)
    }
  }
}

case class MaxOf(left: Expression, right: Expression) extends Expression {
  type EvaluatedType = Any

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable && right.nullable

  override def children: Seq[Expression] = left :: right :: Nil

  override lazy val resolved =
    left.resolved && right.resolved &&
    left.dataType == right.dataType

  override def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }

  private[this] val ordering =
    if (resolved) {
      left.dataType match {
        case i: AtomicType => i.ordering.asInstanceOf[Ordering[Any]]
        case other => UnresolvedNumeric.asInstanceOf[Ordering[Any]]
      }
    } else {
      UnresolvedNumeric.asInstanceOf[Ordering[Any]]
    }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    val evalE2 = right.eval(input)
    if (evalE1 == null) {
      evalE2
    } else if (evalE2 == null) {
      evalE1
    } else {
      if (ordering.compare(evalE1, evalE2) < 0) {
        evalE2
      } else {
        evalE1
      }
    }
  }

  override def toString: String = s"MaxOf($left, $right)"
}

case class MinOf(left: Expression, right: Expression) extends Expression {
  type EvaluatedType = Any

  override def foldable: Boolean = left.foldable && right.foldable

  override def nullable: Boolean = left.nullable && right.nullable

  override def children: Seq[Expression] = left :: right :: Nil

  override lazy val resolved =
    left.resolved && right.resolved &&
    left.dataType == right.dataType

  override def dataType: DataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    left.dataType
  }

  private[this] val ordering =
    if (resolved) {
      left.dataType match {
        case i: AtomicType => i.ordering.asInstanceOf[Ordering[Any]]
        case other => UnresolvedNumeric.asInstanceOf[Ordering[Any]]
      }
    } else {
      UnresolvedNumeric.asInstanceOf[Ordering[Any]]
    }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    val evalE2 = right.eval(input)
    if (evalE1 == null) {
      evalE2
    } else if (evalE2 == null) {
      evalE1
    } else {
      if (ordering.compare(evalE1, evalE2) > 0) {
        evalE2
      } else {
        evalE1
      }
    }
  }

  override def toString: String = s"MinOf($left, $right)"
}

/**
 * A function that get the absolute value of the numeric value.
 */
case class Abs(child: Expression) extends UnaryExpression  {
  type EvaluatedType = Any

  override def dataType: DataType = child.dataType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def toString: String = s"Abs($child)"

  private[this] val numeric =
    if (resolved) {
      dataType match {
        case n: NumericType => n.numeric.asInstanceOf[Numeric[Any]]
        case other => UnresolvedNumeric.asInstanceOf[Numeric[Any]]
      }
    } else {
      UnresolvedNumeric.asInstanceOf[Numeric[Any]]
    }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      numeric.abs(evalE)
    }
  }
}

object UnresolvedNumeric extends Numeric[Nothing] with Integral[Nothing] {
  override def plus(x: Nothing, y: Nothing): Nothing = numericError

  override def toDouble(x: Nothing): Double = numericError

  override def toFloat(x: Nothing): Float = numericError

  override def toInt(x: Nothing): Int = numericError

  override def negate(x: Nothing): Nothing = numericError

  override def fromInt(x: Int): Nothing = numericError

  override def toLong(x: Nothing): Long = numericError

  override def times(x: Nothing, y: Nothing): Nothing = numericError

  override def minus(x: Nothing, y: Nothing): Nothing = numericError

  override def quot(x: Nothing, y: Nothing): Nothing = numericError

  override def rem(x: Nothing, y: Nothing): Nothing = numericError

  private[this] def numericError =
    sys.error(s"Type does not support ordered operations")

  override def compare(x: Nothing, y: Nothing): Int = orderedError

  private[this] def orderedError =
    sys.error(s"Type does not support ordered operations")

  def bitwiseAnd(x: Any, y: Any): Any = bitwiseError("&")

  def bitwiseOr(x: Any, y: Any): Any = bitwiseError("|")

  def bitwiseXor(x: Any, y: Any): Any = bitwiseError("^")

  def bitwiseNot(x: Any): Any = bitwiseError("~")

  private[this] def bitwiseError(op: String) =
    sys.error(s"Type does not support bitwise $op operation")
}
