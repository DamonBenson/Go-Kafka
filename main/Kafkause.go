package main

import (
	"CopyrightTransaction/utils"
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"strconv"
	"time"
)


//作品大类标签定义
const (
	Style      = iota //流派风格
	Language          //歌曲语种
	Emotion           //情感
	Sound             //声音特质
	Instrument        //配器
)

//大类数量和小类数量定义
const (
	BigLabel   = 5
	SmallLabel = 5
	SceneNum   = 10
	MaxPrice   = 400
	MinPrice   = 2000
	MaxTime    = 3
)

//拟定义卖单数量
const (
	SellOrderNum = 1
)

//作品风格标签定义
const (
	Rock      = iota //摇滚
	Metal            //金属
	Quadratic        //二次元
	Fork             //民谣
	Popular          //流行
)

//作品语种标签定义
const (
	Chinese   = iota //国语
	English          //英语
	Janpanese        //日语
	Korean           //韩语
	Cantonese        //粤语
)

//作品情感标签定义
const (
	Sentimental = iota //伤感
	Improvement        //励志
	Excitement         //兴奋
	Lonely             //孤独
	Love               //恋爱
)

//作品声音特质标签定义
const (
	Sweet         = iota //甜美萌系
	Hyperactivity        //高亢激昂
	Bright               //明亮清透
	Low                  //低沉醇厚
	smoke                //不羁烟嗓
)

//作品配器标签定义
const (
	Guitar   = iota //吉他
	Piano           //钢琴
	Violin          //小提琴
	Drum            //鼓
	Keyboard        //键盘
)

//应用场景标签定义
const (
	Advertisement = iota //商业广告
	PCGame               //游戏
	Hardware             //智能硬件
	Activity             //活动现场
	Teleplay             //电视剧
	Movies               //电影
	Comics               //动漫
	APP                  //APP内置
	Videos               //视频音频
	Fullscene            //全场景
)

//授权渠道定义
const (
	Network       = iota //网络
	FullChannel          //全渠道
	ProductLaunch        //产品发布会/年会/品牌活动
	TV                   //电视
	NetworkMovie         //网络大电影/院线电影
)

//授权范围定义
const (
	China = iota //中国
	Asia         //亚洲
	World        //世界
)

//授权时间定义
const (
	HalfYear  = iota //半年
	OneYear          //一年
	ThreeYear        //三年
	Permanent        //永久
)

//BuyOrder 买单结构体
type BuyOrder struct {
	Subbuyorder       []SubBuyOrder              `json:"subbuyorder"`
	LimitPrice        int64                      `json:"limitPrice"`
	TradeStrategy     int8                       `json:"tradeStrategy"`
	AuthorizationInfo map[int]AuthorizationRange `json:"authorizationInfo"`
	TimeStamp         int64                      `json:"timeStamp"`
	Side              int64                      `json:"side"` //类型，0是买单，1是卖单
	BuyIDHash         []byte                     `json:"idHash"`
}

//NewBuyOrder 买方标签需求工厂函数
func NewBuyOrder(subbuyorder []SubBuyOrder, limitprice int64, tradestrategy int8,
	timestamp int64, authorizationinfo map[int]AuthorizationRange) BuyOrder {
	buyorder := BuyOrder{
		Subbuyorder:       subbuyorder,
		LimitPrice:        limitprice,
		TradeStrategy:     tradestrategy,
		AuthorizationInfo: authorizationinfo,
		TimeStamp:         timestamp,
	}
	return buyorder
}

//SubBuyOrder 子买单结构体
type SubBuyOrder struct {
	LabelAmount int64               `json:"labelAmount"`
	LabelDemand map[int][]int       `json:"labelDemand"`
	LabelWeight map[int]map[int]int `json:"labelInfo"`
}

//NewSubBuyOrder 子买单工厂函数
func NewSubBuyOrder(labelamount int64, labeldemand map[int][]int, labelweight map[int]map[int]int) SubBuyOrder {
	subbuyorder := SubBuyOrder{
		LabelAmount: labelamount,
		LabelDemand: labeldemand,
		LabelWeight: labelweight,
	}
	return subbuyorder
}

//AuthorizationRange 授权信息结构体
type AuthorizationRange struct {
	AuthorizationChannel int8  `json:"authorizationChannel"`
	AuthorizationArea    int8  `json:"authorizationArea"`
	AuthorizationTime    int8  `json:"authorizationTime"`
	AuthorizationPrice   int64 `json:"authorizationPrice"`
}

//NewAuthorizationRange 买单工厂函数
func NewAuthorizationRange(authorizationchannel int8, authorizationarea int8, authorizationtime int8,
	authorizationprice int64) AuthorizationRange {
	authorizationrange := AuthorizationRange{
		AuthorizationChannel: authorizationchannel,
		AuthorizationArea:    authorizationarea,
		AuthorizationTime:    authorizationtime,
		AuthorizationPrice:   authorizationprice,
	}
	return authorizationrange
}

//SellOrder 卖单结构体
type SellOrder struct {
	TimeStamp     int64                        `json:"timeStamp"`
	LabelSet      map[int][]int                `json:"labelSet"`
	ExpectedPrice map[int][]AuthorizationRange `json:"expectedPrice"`
	SellIDHash    []byte                       `json:"sellIdHash"`
	MatchScore    float64                      `json:"matchScore"`
}

//NewSellOrder 工厂函数
func NewSellOrder(timestamp int64, labelset map[int][]int, expectedprice map[int][]AuthorizationRange) SellOrder {
	sellorder := SellOrder{
		TimeStamp:     timestamp,
		LabelSet:      labelset,
		ExpectedPrice: expectedprice,
	}
	sellorder.SellIDHash = utils.SetHash(timestamp, expectedprice[Fullscene][len(expectedprice[Fullscene])-1].AuthorizationPrice)
	return sellorder
}

//SellOrderBook 卖单队列
type SellOrderBook struct {
	SellOrder []SellOrder
}

//NewSellOrderBook 工厂模式
func NewSellOrderBook() SellOrderBook {
	sellorderbook := SellOrderBook{}
	return sellorderbook
}

func ParseJson(JsonStr string)(JsonOut *buyOrder){//structFormat *interface{}//TODO根据不同结构体都能解析
	var JsonRaw []byte

	//fmt.Println(JsonStr)
	JsonRaw = []byte(JsonStr)
	//fmt.Println(JsonRaw)
	err := json.Unmarshal(JsonRaw,&JsonOut)
	if err != nil {
		return
	}
	//fmt.Println(JsonOut)
	return  JsonOut
}
// to produce messages
func produce(){

 	conn, err := kafka.DialLeader(context.Background(), "tcp", advertisedHostname, topic, partition)
	if err != nil {
	log.Fatal("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10*time.Second))
	_, err = conn.WriteMessages(
	kafka.Message{Value: []byte("one!")},
	kafka.Message{Value: []byte("two!")},
	kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
	log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
	log.Fatal("failed to close writer:", err)
	}

}
func consumer(){
	// to consume messages

	conn, err := kafka.DialLeader(context.Background(), "tcp", advertisedHostname, topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	conn.SetReadDeadline(time.Now().Add(10*time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
		_, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b))
	}

	if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}
}
func createTopic(AutoEnable bool){
	if(AutoEnable) {// to create topics when auto.create.topics.enable='true'

		_, err := kafka.DialLeader(context.Background(), "tcp", advertisedHostname, topic, 0)
		if err != nil {
			panic(err.Error())
		}
	}else{// to create topics when auto.create.topics.enable='false'


	conn, err := kafka.Dial("tcp", advertisedHostname)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()


	topicConfigs := []kafka.TopicConfig{
		kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
	}
}
func NoLeader(){
	// to connect to the kafka leader via an existing non-leader connection rather than using DialLeader
	conn, err := kafka.Dial("tcp", advertisedHostname)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var connLeader *kafka.Conn
	connLeader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer connLeader.Close()
}
func listTopics(){
	conn, err := kafka.Dial("tcp", advertisedHostname)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	if err != nil {
		panic(err.Error())
	}

	m := map[string]struct{}{}

	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	for k := range m {
		fmt.Println(k)
	}
}

func Reader(offset int64){
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{advertisedHostname},
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(offset)
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
		value := ParseJson(string(m.Value))
		fmt.Println(value)

	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
type Stu struct {
	Name  string `json:"name"`
	Age   int
	High  bool
	sex   string
	Class *Class `json:"class"`
}

type Class struct {
	Name  string
	Grade int
}
type StuRead struct {
	Name  interface{} `json:"name"`
	Age   interface{}
	High  interface{}
	sex   interface{}
	Class interface{} `json:"class"`
	Test  interface{}
}
func ShowTheWayUseJson(){
	//https://blog.csdn.net/zxy_666/article/details/80173288
	//实例化一个数据结构，用于生成json字符串
	stu := Stu{
		Name: "张三",
		Age:  18,
		High: true,
		sex:  "男",
	}

	//指针变量
	cla := new(Class)
	cla.Name = "1班"
	cla.Grade = 3
	stu.Class=cla

	//Marshal失败时err!=nil
	jsonStu, err := json.Marshal(stu)
	if err != nil {
		fmt.Println("生成json字符串错误")
	}

	//jsonStu是[]byte类型，转化成string类型便于查看
	fmt.Println(string(jsonStu))
	//src := string(jsonStu)
	//data:="{\"name\":\"张三\",\"Age\":18,\"high\":true,\"sex\":\"男\",\"CLASS\":{\"Name\":\"1班\",\"Grade\":3}}"
	//str:=[]byte(data)

	//1.Unmarshal的第一个参数是json字符串，第二个参数是接受json解析的数据结构。
	//第二个参数必须是指针，否则无法接收解析的数据，如stu仍为空对象StuRead{}
	//2.可以直接stu:=new(StuRead),此时的stu自身就是指针
	stuaRead := StuRead{}
	stua := Stu{}
	err = json.Unmarshal(jsonStu,&stua)
	err = json.Unmarshal(jsonStu,&stuaRead)

	//解析失败会报错，如json字符串格式不对，缺"号，缺}等。
	if err!=nil{
		fmt.Println(err)
	}

	fmt.Println(stua)
	fmt.Println(stua.Class)
	fmt.Println(stuaRead)
}
//type helloworld struct {
//	Cmd   string
//	Value   string
//}
//type helloworld1 struct {
//	cmd   string
//	value   string
//}

//func LearnParseJson(){
//	data:="{\"name\":\"张三\",\"Age\":18,\"high\":true,\"sex\":\"男\",\"CLASS\":{\"naME\":\"1班\",\"GradE\":3}}"
//	fmt.Println(data)
//	str:=[]byte(data)
//	fmt.Println(str)
//	stua := Stu{}
//	err := json.Unmarshal(str,&stua)
//	fmt.Println(stua)
//
//	data1:="{\"cmd\":\"testRpc\",\"value\":\"Hello World\"}"
//	fmt.Println(data1)
//	str1:=[]byte(data1)
//	fmt.Println(str1)
//	stua1 := helloworld{}
//	err = json.Unmarshal(str1,&stua1)
//	fmt.Println(stua1)
//
//	data0:="{\"cmd\":\"testRpc\",\"value\":\"Hello World\"}"
//	fmt.Println(data0)
//	str0:=[]byte(data0)
//	fmt.Println(str0)
//	stua0 := helloworld1{}
//	err = json.Unmarshal(str0,&stua0)
//	fmt.Println(stua0)
//	if err != nil {
//	  return
//	}
//}