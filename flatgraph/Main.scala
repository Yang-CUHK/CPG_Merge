import io.shiftleft.codepropertygraph.generated.GraphSchema
import io.shiftleft.codepropertygraph.cpgloading.CpgLoader
import io.shiftleft.codepropertygraph.generated.Cpg
//import io.shiftleft.semanticcpg.language._

// import io.shiftleft.codepropertygraph.generated.Cpg.method
import flatgraph.storage.Deserialization
import flatgraph.storage.Serialization
import flatgraph.Graph
import flatgraph.Accessors
import java.nio.file.{Path, Paths}
import flatgraph.storage.cpgMerge
import scala.io.StdIn
import flatgraph.GNode
import flatgraph.Edge
import io.shiftleft.codepropertygraph.generated.nodes
import io.shiftleft.codepropertygraph.generated.language.toGeneratedNodeStarters
import io.shiftleft.codepropertygraph.generated.language.accessPropertyName
import io.shiftleft.codepropertygraph.generated.language.accessPropertyFilename

import flatgraph.{Accessors, Edge, GNode}
import flatgraph.formats.ExportResult
import flatgraph.formats.dot.DotExporter
import flatgraph.formats.graphml.GraphMLExporter
import flatgraph.formats.graphson.GraphSONExporter
import flatgraph.formats.neo4jcsv.Neo4jCsvExporter

// import io.shiftleft.codepropertygraph.generated.Cpg
// import io.shiftleft.codepropertygraph.generated.NodeTypes
// import io.shiftleft.semanticcpg.language.*
// import io.shiftleft.semanticcpg.layers.*




// def splitByMethod(cpg: Cpg): IterableOnce[MethodSubGraph] = {
//   cpg.method.map { method =>
//     MethodSubGraph(methodName = method.name, methodFilename = method.filename, nodes = method.ast.toSet)
//   }
// }

// case class MethodSubGraph(methodName: String, methodFilename: String, nodes: Set[GNode]) {
//   def edges: Set[Edge] = {
//     for {
//       node <- nodes
//       edge <- Accessors.getEdgesOut(node)
//       if nodes.contains(edge.dst)
//     } yield edge
//   }
// }


def main(args: Array[String]): Unit = {
  println("Please enter the location of first CPG:")
  //val filename = StdIn.readLine()
  //println(name)
  val filename = "/home/yang/Desktop/summer/workspace/example-c/cpg.bin"
  val path = Paths.get(filename)
  val absolutePath = path.toAbsolutePath
  //println(absolutePath)
  val tem:Cpg = CpgLoader.load(filename)
  //println(tem)
  val tem1 = Graph.withStorage(GraphSchema,absolutePath)
  //println(tem1)
  val tem2 = Deserialization.readGraph(absolutePath, Option(GraphSchema), true)
  //println(tem2)


  //println("Please enter the location of second CPG:")
  //val newFileName = StdIn.readLine()
  val newFileName = "/home/yang/Desktop/summer/workspace/test-py/cpg.bin"
  val newPath = Paths.get(newFileName)
  val newAbsolutePath = newPath.toAbsolutePath
  //println(newAbsolutePath)
  val tem3 = cpgMerge.readGraph(newAbsolutePath, Option(GraphSchema), tem2,true)
  //println(tem3)
  val tem5 = new Cpg(tem3)
  println(tem5)
  //println(tem5)


  //println("Please enter the location to store the merged CPG:")
  //val storeFileName = StdIn.readLine()
  val storeFileName = "/home/yang/Desktop/summer/The_store_result/cpg.bin"
  val storePath = Paths.get(storeFileName)
  val storeAbsolutePath = storePath.toAbsolutePath
  //println(storeAbsolutePath)
  Serialization.writeGraph(tem3,storeAbsolutePath)

  println("Success")








  //cpgMerge.testHello()
//	//print(GraphSchema)
//	val filename = "/home/yang/Desktop/summer/workspace/example-c/cpg.bin"
//	val path = Paths.get(filename)
//	val absolutePath = path.toAbsolutePath
//	//Deserialization.readGraph(absolutePath, Option(GraphSchema), true)
//  //println(absolutePath)
//
//	val tem:Cpg = CpgLoader.load(filename)
//	println(tem)
//
//	val tem1 = Graph.withStorage(GraphSchema,absolutePath)
//	println(tem1)
//
//	val tem2 = Deserialization.readGraph(absolutePath, Option(GraphSchema), true)
//	println(tem2)

	//val tem3 = Deserialization.cpgMerge(absolutePath, Option(GraphSchema), true)
	//println(tem3)

//	if (CpgLoader.isFlatgraphFormat(absolutePath)) {
//				println("FlatgraphFormat")
//	}
//	if (CpgLoader.isProtoFormat(absolutePath)) {
//		println("ProtoFormat")}
//	} else if (CpgLoader.isOverflowDbFormat(absolutePath)) {
//		println("OverflowDbFormat")
//	} else if (CpgLoader.isFlatgraphFormat(absolutePath)) {
//		println("FlatgraphFormat")
//	} else {
//		println("none")
//	}
//	println("hello world")
//println("HELLO")
	//Graph tem = Graph.withStorage(GraphSchema,absolutePath)
	//println(tem)
}


