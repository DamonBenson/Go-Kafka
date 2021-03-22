package main

// 声明技能结构体
type buyOrder struct {
   OrderId	               string	              //买单标识	Y
   SubBuyOrder	           string	              //子买单	Y
   LimitPrice	           int	                  //限价	Y
   TradeStrategy	       int	                  //交易策略	Y
   AuthorizationInfo	   string	              //授权信息	Y
   Side	                   int	                  //订单类型	Y
   BuyerAddr	           string	              //买方用户地址	Y
   Contact	               string	              //买房用户联系方式	Y
   PlatformAddr	           string	              //买方平台地址	Y
   TimeStamp	           int	                  //时间戳	Y
}
type sellOrder struct {
   Name  string
   Level int
}



const topic = "FormalTest"                      //订阅主题
const advertisedHostname = "39.102.93.47:9092"  //Kafka集群IP
const partition = 0                             //订阅的分区 暂时无用
func main() {
   //BuyOrder []buyOrder//买单队列
   ShowTheWayUseJson()
   //listTopics()
   //Reader(0)
}

