package stu.cfl.bigdata.spark.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor {
    def main(args: Array[String]): Unit = {
        // 启动服务器接受数据
        val server = new ServerSocket(9999)
        println("服务器启动，等待接收数据")

        // 等待客户端的连接
        val client: Socket = server.accept()

        val in: InputStream = client.getInputStream()
        val objIn = new ObjectInputStream(in)
        val task: Task = objIn.readObject().asInstanceOf[Task]
        val list: List[Int] = task.compute()
        println("客户端接受到数据：" + list)

//        val i = in.read()
//        println(i)

        in.close()
        objIn.close()
        client.close()
        server.close()




    }
}
