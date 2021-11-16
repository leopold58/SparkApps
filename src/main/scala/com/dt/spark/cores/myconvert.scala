package com.dt.spark.cores

import org.apache.commons.net.util.Base64


/**
 * @title:
 * @description:
 * @author: jguo
 * @date: 2021/7/28
 */
object myconvert {

  def main(args: Array[String]) {
    val string : String = "abcdefghigklmnopq"
    println("String : " + string)

    // Converting String to byte Array
    // using getBytes method
    val byteArray = string.getBytes

    // printing each value of the byte Array
    println("Byte Array : ")
    for(i <- 0 to byteArray.length-1)
      print(byteArray(i) + "   ")

    println()
    println(byteArray)

    //val byteArrayres = Array[Byte](73, 110, 99, 108, 117, 100, 101, 104, 101, 108, 112)
    val convertedString = new String(byteArray)
    println("The converted string '" + convertedString + "'")

    println("------------------")

    val encodeString = Base64.encodeBase64String(byteArray)

    println(encodeString)

    val decodeBytes = Base64.decodeBase64(encodeString)

    val result = new String(decodeBytes)

    println(result)


  }



}
