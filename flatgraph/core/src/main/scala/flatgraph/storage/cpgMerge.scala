package flatgraph.storage

import com.github.luben.zstd.Zstd
import flatgraph.*
import flatgraph.Edge.Direction
import flatgraph.storage.Manifest.{GraphItem, OutlineStorage}

import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Arrays
import scala.collection.mutable

object cpgMerge{

  def testHello() = {println("hello world")}

  def readGraph(storagePath: Path, schemaMaybe: Option[Schema], existGrapgh:Graph, persistOnClose: Boolean = true): Graph = {
    val fileChannel = new java.io.RandomAccessFile(storagePath.toAbsolutePath.toFile, "r").getChannel
    try {
      // fixme: Use convenience methods from schema to translate string->id. Fix after we get strict schema checking.
      val manifest = GraphItem.read(readManifest(fileChannel))
      val pool     = readPool(manifest, fileChannel)
      val schema   = schemaMaybe.getOrElse(freeSchemaFromManifest(manifest))
      val storagePathMaybe =
        if (persistOnClose) Option(storagePath)
        else None
      val g        = existGrapgh
   
      val nodekinds = mutable.HashMap[String, Short]()
      
      val v        = existGrapgh
     
      for (nodeKind <- g.schema.nodeKinds) nodekinds(g.schema.getNodeLabel(nodeKind)) = {
        nodeKind.toShort
      }
      val kindRemapper = Array.fill(manifest.nodes.size)(-1.toShort)
      val nodeRemapper = new Array[Array[GNode]](manifest.nodes.length)

      //println(manifest.nodes.length)

      val count_Old_Node = new Array[Int](44)

      val count_New_Node = new Array[Int](44)

      for {
        (nodeItem, idx) <- manifest.nodes.zipWithIndex
        nodeKind        <- nodekinds.get(nodeItem.nodeLabel)
      } {
        kindRemapper(idx) = nodeKind

        val existLength = g.nodesArray(nodeKind).length
        count_Old_Node(nodeKind) = existLength
        count_New_Node(nodeKind) = nodeItem.nnodes
        //println(nodeKind)

        val nodes = new Array[GNode](nodeItem.nnodes+existLength)

        val node_tem = g.nodesArray(nodeKind)

        for (seq <- Range(0, nodes.length)) /*println(seq)*/{
          nodes(seq) = g.schema.makeNode(g, nodeKind, seq)
        }

    
        g.nodesArray(nodeKind) = nodes
        nodeRemapper(idx) = nodes
        if (nodeItem.deletions != null) {
          for (del <- nodeItem.deletions) {
            val node = nodes(del)
            if (!AccessHelpers.isDeleted(node)) AccessHelpers.markDeleted(nodes(del))
            else throw new RuntimeException()
          }
          g.livingNodeCountByKind(nodeKind) = nodes.length - nodeItem.deletions.length
        } else g.livingNodeCountByKind(nodeKind) = nodes.length
      }
      //println(g.schema.edgeKinds.length)



      val edgeKinds = mutable.HashMap[(String, String), Short]()
      for {
        nodeKind <- g.schema.nodeKinds
        edgeKind <- g.schema.edgeKinds
      } {
        // println(nodeKind)
        // println(edgeKind)
        // println("---")
        val nodeLabel = g.schema.getNodeLabel(nodeKind)
        val edgeLabel = g.schema.getEdgeLabel(nodeKind, edgeKind)
  
        if (edgeLabel != null) {
          edgeKinds((nodeLabel, edgeLabel)) = edgeKind.toShort

        }
      }

      // for(nodeKind <- 0 to 43){
      //   for(edgeKind <- 0 to 23){
      //     println(nodeKind)
      //     println(edgeKind)
      //     println("----")
      //   }

      // }

      for (edgeItem <- manifest.edges) {
        val nodeKind  = nodekinds.get(edgeItem.nodeLabel)     
        val edgeKind  = edgeKinds.get(edgeItem.nodeLabel, edgeItem.edgeLabel)
        val direction = Direction.fromOrdinal(edgeItem.inout)       
        if (nodeKind.isDefined && edgeKind.isDefined) {
          val pos = g.schema.neighborOffsetArrayIndex(nodeKind.get, direction, edgeKind.get)
          g.neighbors(pos) = deltaDecode(readArray(fileChannel, edgeItem.qty, nodeRemapper, pool).asInstanceOf[Array[Int]])
          g.neighbors(pos + 1) = readArray(fileChannel, edgeItem.neighbors, nodeRemapper, pool)
          //for(i <- g.neighbors(pos+1).asInstanceOf[Array[GNode]]) println(i)
          
          val tem1 = g.neighbors(pos)
          g.neighbors(pos) = deltaDecode(readArray(fileChannel, edgeItem.qty, nodeRemapper, pool).asInstanceOf[Array[Int]])
          var oldLength = 0
          if(tem1 != null){
            oldLength = tem1.asInstanceOf[Array[Int]].length
          }

          var tt = 1
          if(oldLength != 0) tt = oldLength
         
          val newLength = g.neighbors(pos).asInstanceOf[Array[Int]].length
         



          val mergeLength = g.nodesArray(nodeKind.map(_.toInt).getOrElse(0)).length + 1
          //println(tt+newLength)
          //println(mergeLength)

          val mergeArray1 = new Array[Int](mergeLength)
          
          if(tt+newLength - g.nodesArray(nodeKind.map(_.toInt).getOrElse(0)).length == 2){
            for(i <- 0 to oldLength-1){
              mergeArray1(i) = tem1.asInstanceOf[Array[Int]](i)
            }
            //println(oldLength)
            //println(tem1.asInstanceOf[Array[Int]].length)
            if(oldLength == 0){
              for(i <- oldLength to mergeLength-1){
                mergeArray1(i) = g.neighbors(pos).asInstanceOf[Array[Int]](i)
              }
            }else{
              for(i <- oldLength to mergeLength-1){
              // println("---")
              // println(oldLength)
              // println(mergeLength)
              // println(i)
              // println(i-oldLength+1)
              mergeArray1(i) = g.neighbors(pos).asInstanceOf[Array[Int]](i-oldLength+1) + mergeArray1(oldLength-1)
            }
            }
          }else{
            for(i <- mergeLength-newLength to mergeLength -1){
              mergeArray1(i) = g.neighbors(pos).asInstanceOf[Array[Int]](i - mergeLength + newLength)
            }

          }
          // println("---")
          // if(tem1 != null)
          //   for(i <- tem1.asInstanceOf[Array[Int]]) println(i)
          // println("second")
          // if(g.neighbors(pos) != null)
          //   for(i <-  g.neighbors(pos).asInstanceOf[Array[Int]]) println(i)
          // println("third")
          // for(i <- mergeArray1) println(i)

          // println("!!!")
          // if(tt+newLength - g.nodesArray(nodeKind.map(_.toInt).getOrElse(0)).length != 2){
          //   println("start")
          //   println(g.nodesArray(nodeKind.map(_.toInt).getOrElse(0)).length)
          //   println(mergeArray1.length)
          //   println(g.neighbors(pos).asInstanceOf[Array[Int]].length)
          //   println("---")
          //   for(i <- g.neighbors(pos).asInstanceOf[Array[Int]]) println(i)
          //   println("???")
          //   for(i <- mergeArray1) println(i)
          //   //println(tt)
          //   //println("---")
          //   //println(newLength)
          //   //println(g.nodesArray(nodeKind.map(_.toInt).getOrElse(0)).length)
          // }
          g.neighbors(pos) = mergeArray1



 

          var oldLength2 = 0
          val tem2 = g.neighbors(pos+1)
          if(g.neighbors(pos+1) != null){
            oldLength2 = tem2.asInstanceOf[Array[GNode]].length 
          }


          g.neighbors(pos + 1) = readArray(fileChannel, edgeItem.neighbors, nodeRemapper, pool)
          // println("---")
          // for(i <- g.neighbors(pos + 1).asInstanceOf[Array[GNode]]) println(i)

          var newLength2 = 0
          if(g.neighbors(pos+1) != null){
            newLength2 = g.neighbors(pos + 1).asInstanceOf[Array[GNode]].length 
          }

          val mergeLength2 = oldLength2 + newLength2
          val mergeArray2 = new Array[GNode](mergeLength2)
     

          for(i <- 0 to oldLength2-1){
            mergeArray2(i) = tem2.asInstanceOf[Array[GNode]](i)
          }
         
          for(i <- oldLength2 to mergeLength2-1){
            val tem_Node = g.neighbors(pos+1).asInstanceOf[Array[GNode]](i-oldLength2)
            val add_Node = g.nodesArray(tem_Node.nodeKind)(tem_Node.seq + count_Old_Node(tem_Node.nodeKind))
            mergeArray2(i) = add_Node
            // println("---")
            // println(tem_Node)
            // println(add_Node)
            // println(count_Old_Node(mergeArray2(i).nodeKind))
            // println(count_New_Node(mergeArray2(i).nodeKind))
            // println(g.nodesArray(mergeArray2(i).nodeKind).length)
            // println(mergeArray2(i))
          }
          g.neighbors(pos+1) = mergeArray2




  

          var oldLength3 = 0
          val tem3 = g.neighbors(pos+2)
        
          if(tem3 != null){
            if(tem3.isInstanceOf[Array[String]]){
              oldLength3 = tem3.asInstanceOf[Array[String]].length
              
            }else{
              oldLength3 = 1
              
            }

          }


          var newLength3 = 0

          val property = readArray(fileChannel, edgeItem.property, nodeRemapper, pool)
          if (property != null){
            g.neighbors(pos + 2) = property
            newLength3 = property.length
            //println(property.length)
            
          }


          val mergeLength3 = oldLength3 + newLength3
          if(mergeLength3 != 0 && tem3 != null){
            val mergeArray3 = new Array[String](mergeLength3)

            if(tem3.isInstanceOf[Array[String]]){

              for(i <- 0 to oldLength3-1){
                mergeArray3(i) = tem3.asInstanceOf[Array[String]](i)
              }
              //println("world")
              for(i <- oldLength3 to mergeLength3-1){
                // The number of this should be changed
                // println(i)
                // println(i-oldLength)
                mergeArray3(i) = g.neighbors(pos+2).asInstanceOf[Array[String]](i-oldLength3)
              }
              g.neighbors(pos+2) = mergeArray3

            }else{
              val empty = "<empty>"
              mergeArray3(0) = empty
              for(i <- oldLength3 to mergeLength3-1){
                mergeArray3(i) = g.neighbors(pos+2).asInstanceOf[Array[String]](i-oldLength3)
              }
              g.neighbors(pos+2) = mergeArray3
              //println(oldLength3)

            }

          }


        }

      }

      val propertykinds = mutable.HashMap[(String, String), Int]()
      for {
        nodeKind     <- g.schema.nodeKinds
        propertyKind <- g.schema.propertyKinds
      } {
        val nodeLabel     = g.schema.getNodeLabel(nodeKind)
        val propertyLabel = g.schema.getPropertyLabel(nodeKind, propertyKind)
        if (propertyLabel != null) {
          propertykinds((nodeLabel, propertyLabel)) = propertyKind
        }
      }

      for (property <- manifest.properties) {
        val nodeKind     = nodekinds.get(property.nodeLabel)
        //println(nodeKind)
        val propertyKind = propertykinds.get((property.nodeLabel, property.propertyLabel))
        //println(propertyKind)
        if (nodeKind.isDefined && propertyKind.isDefined) {
          val pos = g.schema.propertyOffsetArrayIndex(nodeKind.get, propertyKind.get)

          val tem_pos = g.properties(pos)
          val tem_pos1 = g.properties(pos + 1)
          g.properties(pos) = deltaDecode(readArray(fileChannel, property.qty, nodeRemapper, pool).asInstanceOf[Array[Int]])
          g.properties(pos + 1) = readArray(fileChannel, property.property, nodeRemapper, pool)
          val new_pos = g.properties(pos)
          val new_pos1 = g.properties(pos + 1)

          if(tem_pos1 != null) {
            val oldPropertyLength = tem_pos.asInstanceOf[Array[Int]].length
            val newPropertyLength = new_pos.asInstanceOf[Array[Int]].length
            // println(oldPropertyLength)
            // println(newPropertyLength)
            val merge_pos_length = oldPropertyLength + newPropertyLength - 1

            //first we merge pos
            val merge_pos = new Array[Int](merge_pos_length)
            for(i <- 0 to oldPropertyLength-1){
              //println(tem_pos.asInstanceOf[Array[Int]](i))
              merge_pos(i) = tem_pos.asInstanceOf[Array[Int]](i)
            }
            //println("next")
            for(i <- oldPropertyLength to merge_pos_length-1){
              merge_pos(i) = new_pos.asInstanceOf[Array[Int]](i-oldPropertyLength+1) + merge_pos(oldPropertyLength-1)
            }

            //for(i <-  new_pos.asInstanceOf[Array[Int]]) println(i)
            //println("finally")
            //for(i <- merge_pos) println(i)
            //println("---")
            //mergeArray1(i) = g.neighbors(pos).asInstanceOf[Array[Int]](i-oldLength+1) + mergeArray1(oldLength-1)
            


            //then we merge pos+1
            val old_pos1_length = tem_pos.asInstanceOf[Array[Int]](oldPropertyLength-1)
            val new_pos1_length = new_pos.asInstanceOf[Array[Int]](newPropertyLength-1)
            val merge_pos1_length = merge_pos(merge_pos_length-1)
            if(tem_pos1.isInstanceOf[Array[Int]]){
              val merge_pos1 = new Array[Int](merge_pos1_length)
              //println("first")
              for(i <- 0 to old_pos1_length-1){
                //println(tem_pos1.asInstanceOf[Array[Int]](i))
                merge_pos1(i) = tem_pos1.asInstanceOf[Array[Int]](i)
              }
              // println("second")
              for(i <- 0 to new_pos1_length-1){
                // println(new_pos1.asInstanceOf[Array[Int]](i))
                merge_pos1(i+old_pos1_length) = new_pos1.asInstanceOf[Array[Int]](i)
              }
              // println("third")
              // for(i <- merge_pos1) println(i)
            }else if(tem_pos1.isInstanceOf[Array[Boolean]]){
              val merge_pos1 = new Array[Boolean](merge_pos1_length)
              // println("first")
              for(i <- 0 to old_pos1_length-1){
                // println(tem_pos1.asInstanceOf[Array[Boolean]](i))
                merge_pos1(i) = tem_pos1.asInstanceOf[Array[Boolean]](i)
              }
              // println("second")
              for(i <- 0 to new_pos1_length-1){
                // println(new_pos1.asInstanceOf[Array[Boolean]](i))
                merge_pos1(i+old_pos1_length) = new_pos1.asInstanceOf[Array[Boolean]](i)
              }
              // println("third")
              // for(i <- merge_pos1) println(i)
            }else if(tem_pos1.isInstanceOf[Array[String]]){
              val merge_pos1 = new Array[String](merge_pos1_length)
              // println("first")
              for(i <- 0 to old_pos1_length-1){
                // println(tem_pos1.asInstanceOf[Array[String]](i))
                merge_pos1(i) = tem_pos1.asInstanceOf[Array[String]](i)
              }
              // println("second")
              for(i <- 0 to new_pos1_length-1){
                // println(new_pos1.asInstanceOf[Array[String]](i))
                merge_pos1(i+old_pos1_length) = new_pos1.asInstanceOf[Array[String]](i)
              }
              // println("third")
              // for(i <- merge_pos1) println(i)
            }else{
              //println("hehe")
            }
            



            // println(tem_pos1.getClass)
            // println(g.properties(pos + 1).getClass)
            // println("---")
          }

          // if(tem_pos != null){
          // println("---")
          // println(tem_pos.asInstanceOf[Array[Int]].length)
          // println("content")
          // for(i <- tem_pos.asInstanceOf[Array[Int]]){
          //   println(i)
          // }
          // println("next")
          // if(tem_pos1.isInstanceOf[Array[Int]]){
          //   //println("hello");
          //   println(tem_pos1.asInstanceOf[Array[Int]].length)
          // }else if(tem_pos1.isInstanceOf[Array[Boolean]]){
          //   println(tem_pos1.asInstanceOf[Array[Boolean]].length)
          //   //println("world");
          // }else if(tem_pos1.isInstanceOf[Array[String]]){
          //   println(tem_pos1.asInstanceOf[Array[String]].length)
          //   //println("hehe")
          // }else{
          //   println("????????????????????????????????")
          // }
          // }

          // println("???")
          //println(g.properties(pos + 1).getClass)
        }
      }
      //println(g)
      g
    } finally fileChannel.close()
  }


  private def freeSchemaFromManifest(manifest: Manifest.GraphItem): FreeSchema = {
    val nodeLabels               = manifest.nodes.map { n => n.nodeLabel }
    val nodePropNames            = mutable.LinkedHashMap.empty[String, AnyRef]
    val propertyNamesByNodeLabel = mutable.LinkedHashMap.empty[String, Set[String]]
    for (prop <- manifest.properties) {
      propertyNamesByNodeLabel.updateWith(prop.nodeLabel) {
        case None             => Some(Set(prop.propertyLabel))
        case Some(oldEntries) => Some(oldEntries + prop.propertyLabel)
      }
      nodePropNames(prop.propertyLabel) = protoFromOutline(prop.property)
    }
    val propertyLabels         = nodePropNames.keysIterator.toArray
    val nodePropertyPrototypes = nodePropNames.valuesIterator.toArray

    val edgePropNames = mutable.LinkedHashMap[String, AnyRef]()
    for (edge <- manifest.edges) {
      edgePropNames.get(edge.edgeLabel) match {
        case None | Some(null) => edgePropNames(edge.edgeLabel) = protoFromOutline(edge.property)
        case _                 =>
      }
    }
    val edgeLabels             = edgePropNames.keysIterator.toArray
    val edgePropertyPrototypes = edgePropNames.valuesIterator.toArray

    new FreeSchema(nodeLabels, propertyLabels, nodePropertyPrototypes, propertyNamesByNodeLabel.toMap, edgeLabels, edgePropertyPrototypes)
  }

  private def protoFromOutline(outline: OutlineStorage): AnyRef = {
    if (outline == null) return null
    outline.typ match {
      case StorageType.Bool   => new Array[Boolean](0)
      case StorageType.Byte   => new Array[Byte](0)
      case StorageType.Short  => new Array[Short](0)
      case StorageType.Int    => new Array[Int](0)
      case StorageType.Long   => new Array[Long](0)
      case StorageType.Float  => new Array[Float](0)
      case StorageType.Double => new Array[Double](0)
      case StorageType.Ref    => new Array[GNode](0)
      case StorageType.String => new Array[String](0)
    }
  }

  def readManifest(channel: FileChannel): ujson.Value = {
    if (channel.size() < HeaderSize)
      throw new DeserializationException(s"corrupt file, expected at least $HeaderSize bytes, but only found ${channel.size()}")

    val header    = ByteBuffer.allocate(HeaderSize).order(ByteOrder.LITTLE_ENDIAN)
    var readBytes = 0
    while (readBytes < HeaderSize) {
      readBytes += channel.read(header, readBytes)
    }
    header.flip()

    val headerBytes = new Array[Byte](Keys.Header.length)
    header.get(headerBytes)
    if (!Arrays.equals(headerBytes, Keys.Header))
      throw new DeserializationException(
        s"expected header '$MagicBytesString' (`${Keys.Header.mkString("")}`), but found '${headerBytes.mkString("")}'"
      )

    val manifestOffset = header.getLong()
    val manifestSize   = channel.size() - manifestOffset
    val manifestBytes  = ByteBuffer.allocate(manifestSize.toInt)
    readBytes = 0
    while (readBytes < manifestSize) {
      readBytes += channel.read(manifestBytes, readBytes + manifestOffset)
    }
    manifestBytes.flip()
    ujson.read(manifestBytes)

  }

  private def readPool(manifest: GraphItem, fileChannel: FileChannel): Array[String] = {
    val stringPoolLength = Zstd
      .decompress(
        fileChannel.map(FileChannel.MapMode.READ_ONLY, manifest.stringPoolLength.startOffset, manifest.stringPoolLength.compressedLength),
        manifest.stringPoolLength.decompressedLength
      )
      .order(ByteOrder.LITTLE_ENDIAN)
    val stringPoolBytes = Zstd
      .decompress(
        fileChannel
          .map(FileChannel.MapMode.READ_ONLY, manifest.stringPoolBytes.startOffset, manifest.stringPoolBytes.compressedLength),
        manifest.stringPoolBytes.decompressedLength
      )
      .order(ByteOrder.LITTLE_ENDIAN)
    val poolBytes = new Array[Byte](manifest.stringPoolBytes.decompressedLength)
    stringPoolBytes.get(poolBytes)
    val pool    = new Array[String](manifest.stringPoolLength.decompressedLength >> 2)
    var idx     = 0
    var poolPtr = 0
    while (idx < pool.length) {
      val len = stringPoolLength.getInt()
      pool(idx) = new String(poolBytes, poolPtr, len, StandardCharsets.UTF_8)
      idx += 1
      poolPtr += len
    }
    pool
  }

  private def deltaDecode(a: Array[Int]): Array[Int] = {
    if (a == null) return null
    var idx    = 0
    var cumsum = 0
    while (idx < a.length) {
      val tmp = a(idx)
      a(idx) = cumsum
      cumsum += tmp
      idx += 1
    }
    a
  }

  private def readArray(channel: FileChannel, ptr: OutlineStorage, nodes: Array[Array[GNode]], stringPool: Array[String]): Array[?] = {
    if (ptr == null) return null
    val dec = Zstd
      .decompress(channel.map(FileChannel.MapMode.READ_ONLY, ptr.startOffset, ptr.compressedLength), ptr.decompressedLength)
      .order(ByteOrder.LITTLE_ENDIAN)
    ptr.typ match {
      case StorageType.Bool =>
        val bytes = new Array[Byte](dec.limit())
        dec.get(bytes)
        bytes.map {
          case 0 => false
          case 1 => true
        }
      case StorageType.Byte =>
        val bytes = new Array[Byte](dec.limit())
        dec.get(bytes)
        bytes
      case StorageType.Short =>
        val res = new Array[Short](dec.limit() >> 1)
        dec.asShortBuffer().get(res)
        res
      case StorageType.Int =>
        val res = new Array[Int](dec.limit() >> 2)
        dec.asIntBuffer().get(res)
        res
      case StorageType.Long =>
        val res = new Array[Long](dec.limit() >> 3)
        dec.asLongBuffer().get(res)
        res
      case StorageType.Float =>
        val res = new Array[Float](dec.limit() >> 2)
        dec.asFloatBuffer().get(res)
        res
      case StorageType.Double =>
        val res = new Array[Double](dec.limit() >> 3)
        dec.asDoubleBuffer().get(res)
        res
      case StorageType.String =>
        val res    = new Array[String](dec.limit() >> 2)
        val intbuf = dec.asIntBuffer()
        var idx    = 0
        while (idx < res.length) {
          val offset = intbuf.get(idx)
          if (offset >= 0) res(idx) = stringPool(offset)
          idx += 1
        }
        res
      case StorageType.Ref =>
        val res     = new Array[GNode](dec.limit() >> 3)
        val longbuf = dec.asLongBuffer()
        var idx     = 0
        while (idx < res.length) {
          val encodedRef = longbuf.get()
          val kind       = (encodedRef >>> 32).toInt
          val seqid      = encodedRef.toInt
          if (kind >= 0) {
            if (kind < nodes.length) res(idx) = nodes(kind)(seqid)
            else {
              // we cannot decode this node -- it is a type that doesn't exist in our schema.
              // fixme log message.
            }
          } // otherwise this encodes a null-pointer
          idx += 1
        }
        res
    }
  }

  class DeserializationException(message: String, cause: Option[Throwable] = None) extends RuntimeException(message, cause.orNull)
}