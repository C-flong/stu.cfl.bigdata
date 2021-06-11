package stu.cfl.bigdata.spark.core.test

import org.apache.avro.file.FileReader

import java.io.{FileInputStream, InputStreamReader, ObjectOutputStream, OutputStream}
import java.net.Socket


object Driver {
    def main(args: Array[String]): Unit = {
      val client = new Socket("localhost", 9999)

      val out: OutputStream = client.getOutputStream()
      val objOut = new ObjectOutputStream(out)
//      new FileReader(new File("data"))
      val task: Task = new Task()

      objOut.writeObject(task)
//      out.write(2)
      objOut.flush()
      objOut.close()
      out.close()

      client.close()

    }

}
