package com.util

object RptUtils {
  def requestProcessor(req:Int,pro:Int):List[Double]={
    if(req ==1 && pro ==1){
      List[Double](1,0,0)
    }else if(req ==1 && pro ==2){
      List[Double](1,1,0)
    }else if(req ==1 && pro ==3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }
  def isBidAndWin(
                   iseffective: Int,
                   isbilling: Int,
                   isbid: Int,
                   iswin: Int,
                   adorderid: Int,
                   winprice: Double,
                   adpayment: Double) = {
    if(iseffective == 1 && isbilling ==1 && isbid ==1){
      if(iseffective == 1 && isbilling ==1 && iswin ==1 && adorderid !=0){
        List[Double](1,1,winprice/1000,adpayment/1000)
      }else{
        List[Double](1,0,0,0)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }
  def showAndClk(req:Int,iseffective:Int) ={
    if(req ==2 && iseffective ==1){
      List[Double](1,0)
    }else if(req ==3 && iseffective ==1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }
}
