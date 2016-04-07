package adam

/**
  * Created by edwardcannon on 15/03/2016.
  */

import java.io.File

import htsjdk.samtools.SamReaderFactory
import org.bdgenomics.adam.converters.SAMRecordConverter
import org.bdgenomics.adam.models.{RecordGroupDictionary, SequenceDictionary, SequenceRecord}
import org.bdgenomics.adam.rdd.ADAMContext

class AdamExample1 {

}

object AdamMain {
  def main(args: Array[String]): Unit = {
    val sc = SetUpSpark.configure()
    val ac = new ADAMContext(sc)
    val testRecordConverter = new SAMRecordConverter
    val testFile = new File(args(0))
    val testIterator = SamReaderFactory.makeDefault().open(testFile)
    val testSAMRecord = testIterator.iterator().next()

    val testSequenceRecord1 = SequenceRecord("1", 249250621L)
    val testSequenceRecord2 = SequenceRecord("2", 243199373L)

    // SequenceDictionary to be used as parameter during conversion
    val testSequenceDict = SequenceDictionary(testSequenceRecord1, testSequenceRecord2)

    // RecordGroupDictionary to be used as a parameter during conversion
    val testRecordGroupDict = new RecordGroupDictionary(Seq())

    // Convert samRecord to alignmentRecord
    val testAlignmentRecord = testRecordConverter.convert(testSAMRecord, testSequenceDict, testRecordGroupDict)

 }
}
