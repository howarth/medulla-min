package main.scala.core
/*
object TypeStringToDataIdentifier{
  def apply(typeString : String, identifier : String) : DataId = {

  }
}
*/
/*
TODO: Why doesn't this work?
final class IdDecimal(val bd : BigDecimal, override val mc : MathContext) extends BigDecimal(bd, mc){
  def this(bd : BigDecimal) = this(bd, BigDecimal.defaultMathContext)
}
*/
object IdUtils {
  def truncatedBigDecimal(bd: BigDecimal): BigDecimal = {
    BigDecimal(bd.underlying.stripTrailingZeros())
  }
}

trait Id {
  val id : String
  override def toString: String = this.id
  override def equals(that: Any): Boolean = {
    this.getClass == that.getClass && this.toString == that.toString
  }
  override def hashCode(): Int = this.id.hashCode
}

abstract class DataId extends Id {
  val sep = ":"
  val fileSep = "_"
  lazy val ancestors = this.toString.split(sep).map(ParentId(_))
}

trait SubsetOfDataId extends DataId {
  final val subsetSep = '#'
  val fullDataId : DataId
  val fullDataIdStr = id.split(subsetSep)(0)
}

class TypelessDataId(val id : String) extends DataId
class ParentId(val id : String) extends Id
object ParentId{def apply(id : String) = new ParentId(id)}

class StringId(val id : String) extends DataId
object StringId{def apply(id : String) = new StringId(id)}

/*
  Scalars
*/
abstract class ScalarId(val id : String) extends DataId

class DoubleScalarId(override val id : String) extends ScalarId(id)
class IntScalarId(override val id : String) extends ScalarId(id)
class BooleanScalarId(override val id : String) extends ScalarId(id)

object DoubleScalarId{def apply(id: String) = new DoubleScalarId(id)}
object IntScalarId{def apply(id: String) = new IntScalarId(id)}
object BooleanScalarId{def apply(id: String) = new BooleanScalarId(id)}

/*
  Vectors
 */
abstract class VectorId(val id : String) extends DataId
class DoubleVectorId(override val id : String) extends VectorId(id)
class IntVectorId(override val id : String) extends VectorId(id)
class BooleanVectorId(override val id : String) extends VectorId(id)
class StringVectorId(override val id : String) extends VectorId(id)

object DoubleVectorId{def apply(id: String) = new DoubleVectorId(id)}
object IntVectorId{def apply(id: String) = new IntVectorId(id)}
object BooleanVectorId{def apply(id: String) = new BooleanVectorId(id)}
object StringVectorId{def apply(id: String) = new StringVectorId(id)}

/*
  Matrices
 */
abstract class Matrix2DId(val id : String) extends DataId
class DoubleMatrix2DId(override val id : String) extends Matrix2DId(id)
class IntMatrix2DId(override val id : String) extends Matrix2DId(id)
class BooleanMatrix2DId(override val id : String) extends Matrix2DId(id)

object DoubleMatrix2DId{def apply(id: String) = new DoubleMatrix2DId(id)}
object IntMatrix2DId{def apply(id: String) = new IntMatrix2DId(id)}
object BooleanMatrix2DId{def apply(id: String) = new BooleanMatrix2DId(id)}

class TimeSeriesId(val id : String) extends DataId
class TimeSeriesChannelId( val id : String) extends DataId
object TimeSeriesChannelId{def apply(id: String) = new TimeSeriesChannelId(id)}

class SingleChannelTimeSeriesId(id: String) extends TimeSeriesId(id)
class DoubleSingleChannelTimeSeriesId(override val id : String) extends SingleChannelTimeSeriesId(id)
class IntSingleChannelTimeSeriesId(override val id : String) extends SingleChannelTimeSeriesId(id)
class BooleanSingleChannelTimeSeriesId(override val id : String) extends SingleChannelTimeSeriesId(id)

object DoubleSingleChannelTimeSeriesId{def apply(id: String) = new DoubleSingleChannelTimeSeriesId(id)}
object IntSingleChannelTimeSeriesId{def apply(id: String) = new IntSingleChannelTimeSeriesId(id)}
object BooleanSingleChannelTimeSeriesId{def apply(id: String) = new BooleanSingleChannelTimeSeriesId(id)}

class MultiChannelTimeSeriesId(id: String) extends TimeSeriesId(id)
class DoubleMultiChannelTimeSeriesId(override val id : String) extends MultiChannelTimeSeriesId(id)
class IntMultiChannelTimeSeriesId(override val id : String) extends MultiChannelTimeSeriesId(id)
class BooleanMultiChannelTimeSeriesId(override val id : String) extends MultiChannelTimeSeriesId(id)

object DoubleMultiChannelTimeSeriesId{def apply(id: String) = new DoubleMultiChannelTimeSeriesId(id)}
object IntMultiChannelTimeSeriesId{def apply(id: String) = new IntMultiChannelTimeSeriesId(id)}
object BooleanMultiChannelTimeSeriesId{def apply(id: String) = new BooleanMultiChannelTimeSeriesId(id)}

abstract class MultiChannelTimeSeriesWindowId(val id: String) extends SubsetOfDataId
class DoubleMultiChannelTimeSeriesWindowId(override val id : String) extends MultiChannelTimeSeriesWindowId(id){
  override lazy val fullDataId: DataId = DoubleMultiChannelTimeSeriesId(this.fullDataIdStr)
}
class IntMultiChannelTimeSeriesWindowId(override val id : String) extends MultiChannelTimeSeriesWindowId(id){
  override lazy val fullDataId: DataId = IntMultiChannelTimeSeriesId(this.fullDataIdStr)
}
class BooleanMultiChannelTimeSeriesWindowId(override val id : String) extends MultiChannelTimeSeriesWindowId(id){
  override lazy val fullDataId: DataId = BooleanMultiChannelTimeSeriesId(this.fullDataIdStr)
}

object DoubleMultiChannelTimeSeriesWindowId{def apply(id: String) = new DoubleMultiChannelTimeSeriesWindowId(id)}
object IntMultiChannelTimeSeriesWindowId{def apply(id: String) = new IntMultiChannelTimeSeriesWindowId(id)}
object BooleanMultiChannelTimeSeriesWindowId{def apply(id: String) = new BooleanMultiChannelTimeSeriesWindowId(id)}

class Timestamp(t : BigDecimal) extends Ordered[Timestamp] {
  val underlyingBD: BigDecimal = IdUtils.truncatedBigDecimal(t)
  override def equals(that: Any): Boolean = that match {
    case that: Timestamp => underlyingBD equals that.underlyingBD
    case _ => false
  }
  override def hashCode: Int = underlyingBD.hashCode
  override def toString: String = underlyingBD.toString
  override def compare(that: Timestamp) = {
    this.underlyingBD.compare(that.underlyingBD)
  }
}

object Timestamp {
  def apply(timeBigDecimal: BigDecimal) : Timestamp = new Timestamp(timeBigDecimal)
  def apply(timeString: String) : Timestamp = new Timestamp(BigDecimal(timeString))
}

/*
trait MetadataAttrId extends Id
class SubjectId(val id: String) extends MetadataAttrId
class ExperimentId(val id: String) extends MetadataAttrId
class BlockId(val id: String) extends MetadataAttrId
class Stimulus(val id: String) extends MetadataAttrId
class StimuliSetId(val id: String) extends MetadataAttrId
// Preprocessing Readable ID
class ProcessSlugId(val id: String) extends MetadataAttrId
*/
