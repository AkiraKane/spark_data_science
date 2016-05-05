package chem

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import utils.SetUpSpark
import scala.io.Source._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.explode

/**
  * Created by edwardcannon on 23/04/2016.
  */
class PubChemExtractor {
  val compound_id_baseUrl = "http://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/"
  val properties_list = List[String]("MolecularFormula","MolecularWeight",
  "CanonicalSMILES","IsomericSMILES","InChI","InChIKey","IUPACName",
  "XLogP","ExactMass","MonoisotopicMass","TPSA","Complexity","Charge",
  "HBondDonorCount","HBondAcceptorCount","RotatableBondCount","HeavyAtomCount",
    "IsotopeAtomCount","AtomStereoCount","DefinedAtomStereoCount","UndefinedAtomStereoCount",
  "BondStereoCount","DefinedBondStereoCount","UndefinedBondStereoCount","CovalentUnitCount",
  "Volume3D","XStericQuadrupole3D","YStericQuadrupole3D","ZStericQuadrupole3D","FeatureCount3D",
  "FeatureAcceptorCount3D","FeatureDonorCount3D","FeatureAnionCount3D","FeatureCationCount3D",
  "FeatureRingCount3D","FeatureHydrophobeCount3D","ConformerModelRMSD3D","EffectiveRotorCount3D",
  "ConformerCount3D","Fingerprint2D")
  /**
    * Returns the text content from a REST URL. Returns a blank String if there
    * is a problem.
    */
  def getRestContent(url:String): String = {
    val httpClient = new DefaultHttpClient()
    val httpResponse = httpClient.execute(new HttpGet(url))
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    httpClient.getConnectionManager().shutdown()
    return content
  }
}

object ExtractorMain {
  def main(args: Array[String]): Unit = {
    val sc = SetUpSpark.configure()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val x = new PubChemExtractor()
    val cids = args(0).toInt to args(1).toInt
    var pubchem_json = scala.collection.mutable.ArrayBuffer.empty[String]
    for (id <- cids){
      val url = x.compound_id_baseUrl+id+"/property/"+x.properties_list.mkString(",")+"/JSON"
      val result = x.getRestContent(url)
      pubchem_json += result
    }
    pubchem_json.foreach(println)
    val rdd_pubchem = sc.parallelize(pubchem_json)
    val df = sqlContext.read.json(rdd_pubchem)
    print(df.printSchema())

    val properties_df = df.select(explode(df("PropertyTable.Properties"))).toDF("Properties")
    properties_df.printSchema()
    println(properties_df.show())
    properties_df.select("Properties.IUPACName").foreach(println) //this extracts the iupac names!
    //Extract canonical smiles only
    val canonical_isomeric_SMILES_df = df.select(explode(df("PropertyTable.Properties.CanonicalSMILES"))).toDF("CanonicalIsomericSMILES")
    canonical_isomeric_SMILES_df.printSchema()
    println(canonical_isomeric_SMILES_df.show())


  }
}
