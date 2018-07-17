package adapter

import (
	//	"fmt"
	//	"strconv"
	"time"

	"github.com/alecthomas/log4go"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type MongoDB struct {
	MogSession *mgo.Session
	Collection *mgo.Collection
	ConnFlag   int
}

var mogConn *MongoDB

type Position struct {
	Id_    bson.ObjectId `bson:"_id"`
	Mac    string        `bson:"mac"` //bson:"mac" 表示mongodb数据库中对应的字段名称
	SubMac string        `bson:"subMac"`
	City   string        `bson:"city"`
	//	Latitude  string        `bson:"latitude"`
	//	Longitude string        `bson:"longitude"`
}

/**************************************************************************
【函数】：NewMgoClient()
【功能】:连接mongodb，更新MongoDB struct
【传入参数】:host, dbname, colname string,poolsize int
【输出参数】:(*MongoDB, error)
【创建时间】：2018.06.19 by xjy
****************************************************************************/
func NewMgoClient(uri, dbname, colname string) (*MongoDB, error) {
	if mogConn == nil {
		session, err := mgo.DialWithTimeout(uri, 1000*time.Millisecond)
		if err != nil {
			log4go.Error("mongo error:", err.Error())
			time.Sleep(1000 * time.Millisecond)
			return NewMgoClient(uri, dbname, colname)
		}
		log4go.Info("Mongo initialize ok")
		mogSession := session.Clone()
		//	defer mogSession.Close()
		//mogSession.SetPoolLimit(poolsize)
		collection := mogSession.DB(dbname).C(colname)
		mogConn = &MongoDB{
			MogSession: mogSession,
			Collection: collection,
			ConnFlag:   1,
		}

	}

	return mogConn, nil
}

/**************************************************************************
【函数】：MgoQuery()
【功能】:查询mac是否存在mongodb中
【传入参数】:mac string
【输出参数】:string（city）, error（数据库错误）
【创建时间】：2018.06.19 by xjy
****************************************************************************/
func (p *MongoDB) MgoQuery(mac, subMac string) (string, error) {
	result := Position{}
	err := p.Collection.Find(bson.M{"mac": mac, "subMac": subMac}).One(&result)
	if err != nil {
		if err.Error() == "not found" {
			return "not_found", nil
		}

		return "", err
	}
	return result.City, nil
}

/**************************************************************************
【函数】：MgoAdd()
【功能】:查询mongodb，存在更新，不存在添加
【传入参数】:mac, city string
【输出参数】:bool（true新增或更新成功,false表示已缓存该数据）, error（数据库错误）
【创建时间】：2018.06.19 by xjy
****************************************************************************/
func (p *MongoDB) MgoAdd(mac, subMac, city string) (bool, error) {
	//查询是否存在
	city_query, err := p.MgoQuery(mac, subMac)
	//not found
	if err == nil && city_query == "not_found" {
		position := Position{}
		position.Id_ = bson.NewObjectId()
		position.Mac = mac
		position.SubMac = subMac
		position.City = city
		//新增
		if err := p.Collection.Insert(position); err != nil {
			log4go.Error("mongo add error:%s", err.Error())
			return false, err
		}
		return true, nil
	} else if err == nil && city_query != city {
		//city_query != city 直接更新
		if err := p.Collection.Update(bson.M{"mac": mac, "subMac": subMac}, bson.M{"$set": bson.M{"city": city}}); err != nil {
			log4go.Error("mongo update error:%s", err.Error())
			return false, err
		}
		return true, nil
	}

	return false, err

}

/**************************************************************************
【函数】：ReConn()
【功能】:断开重连机制，每隔1s刷新
【传入参数】:
【输出参数】:
【创建时间】：2018.06.19 by xjy
****************************************************************************/
func (p *MongoDB) ReConn() {

	go func() {
		for {
			p.MogSession.Refresh()
			err := p.MogSession.Ping()
			if err != nil {
				p.ConnFlag = 0
				log4go.Error("mongodb error:", err.Error())
			}
			if err == nil {
				if p.ConnFlag == 0 {
					log4go.Info("ReConnect mongodb ok")
					p.ConnFlag = 1
				}
			}
			time.Sleep(1000 * time.Millisecond)
		}
	}()

}
