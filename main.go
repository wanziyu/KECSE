package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type message struct {
	PMU_ID    int
	Timestamp int64
	Mag_IA1   float64
	Mag_IB1   float64
	Mag_IC1   float64
	Mag_VA1   float64
	Mag_VB1   float64
	Mag_VC1   float64
	Phase_IA1 float64
	Phase_IB1 float64
	Phase_IC1 float64
	Phase_VA1 float64
	Phase_VB1 float64
	Phase_VC1 float64
}

/*type PowerResult struct {
	PMU_ID int
	Timestamp int64
	ActPower_A float64
	ReaPower_A float64
	AppPower_A float64
	ActPower_B float64
	ReaPower_B float64
	AppPower_B float64
	ActPower_C float64
	ReaPower_C float64
	AppPower_C float64
	ActPower_Sum float64
	ReaPower_Sum float64
	AppPower_Sum float64
	PF float64
}*/

/*type BatchResult struct {
	PMU_ID int
	Timestamp int32
	ActAverage float64
	ReaAverage float64
	AppAverage float64
	ActHigh float64
	ReaHigh float64
	AppHigh float64
	ActLow float64
	ReaLow float64
	AppLow float64
}*/

func main() {

	/*	MVA1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",6)
		PVA1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",7)
		MVB1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",10)
		PVB1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",11)
		MVC1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",14)
		PVC1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",15)
		MIA1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 8)
		PIA1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 9)
		MIB1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 12)
		PIB1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 13)
		MIC1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 16)
		PIC1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 17)


		MVA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 6)
		PVA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 7)
		MVB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 10)
		PVB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 11)
		MVC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 14)
		PVC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 15)
		MIA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 18)
		PIA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 19)
		MIB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 20)
		PIB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 21)
		MIC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 22)
		PIC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 23)

		MVA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 6)
		PVA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 7)
		MVB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 10)
		PVB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 11)
		MVC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 14)
		PVC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 15)
		MIA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 8)
		PIA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 9)
		MIB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 12)
		PIB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 13)
		MIC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 16)
		PIC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 17)

		MVA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 6)
		PVA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 7)
		MVB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 10)
		PVB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 11)
		MVC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 14)
		PVC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 15)
		MIA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 8)
		PIA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 9)
		MIB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 12)
		PIB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 13)
		MIC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 16)
		PIC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 17)

		MVA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 6)
		PVA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 7)
		MVB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 10)
		PVB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 11)
		MVC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 14)
		PVC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 15)
		MIA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 8)
		PIA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 9)
		MIB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 12)
		PIB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 13)
		MIC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 16)
		PIC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 17)

		MVA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 6)
		PVA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 7)
		MVB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 10)
		PVB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 11)
		MVC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 14)
		PVC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 15)
		MIA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 8)
		PIA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 9)
		MIB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 12)
		PIB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 13)
		MIC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 16)
		PIC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 17)*/
	//读取三相电流 三相电压数据
	//SOC := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 0)
	//FRACSEC := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 1)
	//PMU1
	/*	MVA1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",6)
		PVA1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",7)
		MVB1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",10)
		PVB1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",11)
		MVC1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",14)
		PVC1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv",15)
		MIA1 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 8)
		PIA1 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 9)
		MIB1 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 12)
		PIB1 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 13)
		MIC1 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 16)
		PIC1 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 17)*/
	/*	//PMU2
		MVA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 6)
		PVA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 7)
		MVB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 10)
		PVB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 11)
		MVC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 14)
		PVC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 15)
		MIA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 18)
		PIA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 19)
		MIB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 20)
		PIB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 21)
		MIC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 22)
		PIC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 23)*/

	//PMU3
	/*MVA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 6)
	PVA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 7)
	MVB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 10)
	PVB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 11)
	MVC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 14)
	PVC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 15)
	MIA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 8)
	PIA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 9)
	MIB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 12)
	PIB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 13)
	MIC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 16)
	PIC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 17)

	//PMU4
	MVA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 6)
	PVA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 7)
	MVB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 10)
	PVB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 11)
	MVC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 14)
	PVC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 15)
	MIA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 8)
	PIA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 9)
	MIB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 12)
	PIB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 13)
	MIC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 16)
	PIC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 17)

	//PMU5
	MVA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 6)
	PVA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 7)
	MVB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 10)
	PVB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 11)
	MVC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 14)
	PVC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 15)
	MIA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 8)
	PIA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 9)
	MIB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 12)
	PIB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 13)
	MIC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 16)
	PIC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 17)

	MVA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 6)
	PVA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 7)
	MVB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 10)
	PVB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 11)
	MVC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 14)
	PVC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 15)
	MIA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 8)
	PIA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 9)
	MIB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 12)
	PIB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 13)
	MIC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 16)
	PIC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 17)*/

	//Clinet 1
	/*	opts1 := MQTT.NewClientOptions().AddBroker("tcp://192.168.1.102:31183")
		//opts.SetClientID(fmt.Spr1intf("go-simple-client:%d", taskId))
		opts1.SetClientID("PMU1")
		opts1.SetDefaultPublishHandler(f)
		opts1.SetConnectTimeout(time.Duration(60) * time.Second)
		//创建连接
		c1:= MQTT.NewClient(opts1)
	*/

	/*
		opts2 := MQTT.NewClientOptions().AddBroker("tcp://192.168.1.102:31183")
		//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
		opts2.SetClientID("PMU2")
		opts2.SetDefaultPublishHandler(f)
		opts2.SetConnectTimeout(time.Duration(60) * time.Second)
		//创建连接
		c2:= MQTT.NewClient(opts2)

		opts3 := MQTT.NewClientOptions().AddBroker("tcp://192.168.1.102:31183")
		//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
		opts3.SetClientID("PMU3")
		opts3.SetDefaultPublishHandler(f)
		opts3.SetConnectTimeout(time.Duration(60) * time.Second)
		//创建连接
		c3:= MQTT.NewClient(opts3)

		opts4 := MQTT.NewClientOptions().AddBroker("tcp://192.168.1.102:31183")
		//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
		opts4.SetClientID("PMU4")
		opts4.SetDefaultPublishHandler(f)
		opts4.SetConnectTimeout(time.Duration(60) * time.Second)
		//创建连接
		c4:= MQTT.NewClient(opts1)

		opts5 := MQTT.NewClientOptions().AddBroker("tcp://192.168.1.102:31183")
		//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
		opts5.SetClientID("PMU5")
		opts5.SetDefaultPublishHandler(f)
		opts5.SetConnectTimeout(time.Duration(60) * time.Second)
		//创建连接
		c5:= MQTT.NewClient(opts1)



		opts6 := MQTT.NewClientOptions().AddBroker("tcp://192.168.1.102:31183")
		//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
		opts6.SetClientID("PMU6")
		opts6.SetDefaultPublishHandler(f)
		opts6.SetConnectTimeout(time.Duration(60) * time.Second)
		//创建连接
		c6:= MQTT.NewClient(opts6)

	*/var num = 3
	wg := sync.WaitGroup{}
	wg.Add(num)
	var mqttaddress string = "tcp://10.66.101.210:31183"
	//transwg := sync.WaitGroup{
	//t1 := time.Now()
	/*	for i:=0;i<len(MVA1);i++{
		wg1.Add(6)
		time.Sleep(20 * time.Millisecond)
		//timestamp := SOC[i]+"000"
		//times,_:=strconv.ParseInt(timestamp, 10, 64)
		//ms,_ := strconv.ParseInt(FRACSEC[i], 10, 64)
		mva1,_ := strconv.ParseFloat(MVA1[i],64)
		pva1,_ := strconv.ParseFloat(PVA1[i],64)
		mvb1,_ := strconv.ParseFloat(MVB1[i],64)
		pvb1,_ := strconv.ParseFloat(PVB1[i],64)
		mvc1,_ := strconv.ParseFloat(MVC1[i],64)
		pvc1,_ := strconv.ParseFloat(PVC1[i],64)
		mia1,_ := strconv.ParseFloat(MIA1[i],64)
		pia1,_ := strconv.ParseFloat(PIA1[i],64)
		mib1,_ := strconv.ParseFloat(MIB1[i],64)
		pib1,_ := strconv.ParseFloat(PIB1[i],64)
		mic1,_ := strconv.ParseFloat(MIC1[i],64)
		pic1,_ := strconv.ParseFloat(PIC1[i],64)*/

	/*		mva2,_ := strconv.ParseFloat(MVA2[i],64)
			pva2,_ := strconv.ParseFloat(PVA2[i],64)
			mvb2,_ := strconv.ParseFloat(MVB2[i],64)
			pvb2,_ := strconv.ParseFloat(PVB2[i],64)
			mvc2,_ := strconv.ParseFloat(MVC2[i],64)
			pvc2,_ := strconv.ParseFloat(PVC2[i],64)
			mia2,_ := strconv.ParseFloat(MIA2[i],64)
			pia2,_ := strconv.ParseFloat(PIA2[i],64)
			mib2,_ := strconv.ParseFloat(MIB2[i],64)
			pib2,_ := strconv.ParseFloat(PIB2[i],64)
			mic2,_ := strconv.ParseFloat(MIC2[i],64)
			pic2,_ := strconv.ParseFloat(PIC2[i],64)*/
	/*
		mva3,_ := strconv.ParseFloat(MVA3[i],64)
		pva3,_ := strconv.ParseFloat(PVA3[i],64)
		mvb3,_ := strconv.ParseFloat(MVB3[i],64)
		pvb3,_ := strconv.ParseFloat(PVB3[i],64)"tcp://192.168.1.103:31183"
		mvc3,_ := strconv.ParseFloat(MVC3[i],64)
		pvc3,_ := strconv.ParseFloat(PVC3[i],64)
		mia3,_ := strconv.ParseFloat(MIA3[i],64)
		pia3,_ := strconv.ParseFloat(PIA3[i],64)
		mib3,_ := strconv.ParseFloat(MIB3[i],64)
		pib3,_ := strconv.ParseFloat(PIB3[i],64)
		mic3,_ := strconv.ParseFloat(MIC3[i],64)
		pic3,_ := strconv.ParseFloat(PIC3[i],64)

		mva4,_ := strconv.ParseFloat(MVA4[i],64)
		pva4,_ := strconv.ParseFloat(PVA4[i],64)
		mvb4,_ := strconv.ParseFloat(MVB4[i],64)
		pvb4,_ := strconv.ParseFloat(PVB4[i],64)
		mvc4,_ := strconv.ParseFloat(MVC4[i],64)
		pvc4,_ := strconv.ParseFloat(PVC4[i],64)
		mia4,_ := strconv.ParseFloat(MIA4[i],64)
		pia4,_ := strconv.ParseFloat(PIA4[i],64)
		mib4,_ := strconv.ParseFloat(MIB4[i],64)
		pib4,_ := strconv.ParseFloat(PIB4[i],64)
		mic4,_ := strconv.ParseFloat(MIC4[i],64)
		pic4,_ := strconv.ParseFloat(PIC4[i],64)

		mva5,_ := strconv.ParseFloat(MVA5[i],64)
		pva5,_ := strconv.ParseFloat(PVA5[i],64)
		mvb5,_ := strconv.ParseFloat(MVB5[i],64)
		pvb5,_ := strconv.ParseFloat(PVB5[i],64)
		mvc5,_ := strconv.ParseFloat(MVC5[i],64)
		pvc5,_ := strconv.ParseFloat(PVC5[i],64)
		mia5,_ := strconv.ParseFloat(MIA5[i],64)
		pia5,_ := strconv.ParseFloat(PIA5[i],64)
		mib5,_ := strconv.ParseFloat(MIB5[i],64)
		pib5,_ := strconv.ParseFloat(PIB5[i],64)
		mic5,_ := strconv.ParseFloat(MIC5[i],64)
		pic5,_ := strconv.ParseFloat(PIC5[i],64)

		mva6,_ := strconv.ParseFloat(MVA6[i],64)
		pva6,_ := strconv.ParseFloat(PVA6[i],64)
		mvb6,_ := strconv.ParseFloat(MVB6[i],64)
		pvb6,_ := strconv.ParseFloat(PVB6[i],64)
		mvc6,_ := strconv.ParseFloat(MVC6[i],64)
		pvc6,_ := strconv.ParseFloat(PVC6[i],64)
		mia6,_ := strconv.ParseFloat(MIA6[i],64)
		pia6,_ := strconv.ParseFloat(PIA6[i],64)
		mib6,_ := strconv.ParseFloat(MIB6[i],64)
		pib6,_ := strconv.ParseFloat(PIB6[i],64)
		mic6,_ := strconv.ParseFloat(MIC6[i],64)
		pic6,_ := strconv.ParseFloat(PIC6[i],64)*/

	/*Message1 := message{
				PMU_ID: 1,
				Timestamp: time.Now().UnixNano() / 1e6,
				Mag_VA1: mva1,
	            Phase_VA1: pva1,
				Mag_VB1: mvb1,
				Phase_VB1: pvb1,
				Mag_VC1: mvc1,
				Phase_VC1: pvc1,
				Mag_IA1: mia1,
				Phase_IA1: pia1,
				Mag_IB1: mib1,
				Phase_IB1: pib1,
				Mag_IC1: mic1,
				Phase_IC1: pic1,
			}
			Message2 := message{
				PMU_ID: 2,
				Timestamp: time.Now().UnixNano() / 1e6,
				Mag_VA1: mva2,
				Phase_VA1: pva2,
				Mag_VB1: mvb2,
				Phase_VB1: pvb2,
				Mag_VC1: mvc2,
				Phase_VC1: pvc2,
				Mag_IA1: mia2,
				Phase_IA1: pia2,
				Mag_IB1: mib2,
				Phase_IB1: pib2,
				Mag_IC1: mic2,
				Phase_IC1: pic2,
			}

			Message3 := message{
				PMU_ID: 3,
				Timestamp: time.Now().UnixNano() / 1e6,
				Mag_VA1: mva3,
				Phase_VA1: pva3,
				Mag_VB1: mvb3,
				Phase_VB1: pvb3,
				Mag_VC1: mvc3,
				Phase_VC1: pvc3,
				Mag_IA1: mia3,
				Phase_IA1: pia3,
				Mag_IB1: mib3,
				Phase_IB1: pib3,
				Mag_IC1: mic3,
				Phase_IC1: pic3,
			}

			Message4 := message{
				PMU_ID: 4,
				Timestamp: time.Now().UnixNano() / 1e6,
				Mag_VA1: mva4,
				Phase_VA1: pva4,
				Mag_VB1: mvb4,
				Phase_VB1: pvb4,
				Mag_VC1: mvc4,
				Phase_VC1: pvc4,
				Mag_IA1: mia4,
				Phase_IA1: pia4,
				Mag_IB1: mib4,
				Phase_IB1: pib4,
				Mag_IC1: mic4,
				Phase_IC1: pic4,
			}

			Message5 := message{
				PMU_ID: 5,
				Timestamp: time.Now().UnixNano() / 1e6,
				Mag_VA1: mva5,
				Phase_VA1: pva5,
				Mag_VB1: mvb5,
				Phase_VB1: pvb5,
				Mag_VC1: mvc5,
				Phase_VC1: pvc5,
				Mag_IA1: mia5,
				Phase_IA1: pia5,
				Mag_IB1: mib5,
				Phase_IB1: pib5,
				Mag_IC1: mic5,
				Phase_IC1: pic5,
			}

			Message6 := message{
				PMU_ID: 6,
				Timestamp: time.Now().UnixNano() / 1e6,
				Mag_VA1: mva6,
				Phase_VA1: pva6,
				Mag_VB1: mvb6,
				Phase_VB1: pvb6,
				Mag_VC1: mvc6,
				Phase_VC1: pvc6,
				Mag_IA1: mia6,
				Phase_IA1: pia6,
				Mag_IB1: mib6,
				Phase_IB1: pib6,
				Mag_IC1: mic6,
				Phase_IC1: pic6,
			}*/
	/*		Batch := BatchResult{
				PMU_ID: 1,
				Timestamp: 1234567890,
				ActAverage: mva,
				ReaAverage: mva,
				AppAverage: mva,
				ActHigh: mva,
				ReaHigh: mva,
				AppHigh: mva,
				ActLow: mva,
				ReaLow: mva,
				AppLow: mva,
			}
			bat,error:= json.Marshal(Batch)
			if error != nil{
				fmt.Println("Json Error:", error)
			}
			fmt.Println(string(bat))*/
	/*		Power := PowerResult{
			PMU_ID: 1,
			Timestamp: int64(time.Now().UnixNano() / 1e6),
			ActPower_A: 5523.11,
			ReaPower_A: -2234.232,
			AppPower_A: 5823.13,
			ActPower_B: 5523.11,
			ReaPower_B: -2234.232,
			AppPower_B: 5823.13,
			ActPower_C: 5523.11,
			ReaPower_C: -2234.232,
			AppPower_C: 5823.13,
			ActPower_Sum: 5523.11,
			ReaPower_Sum: -2234.232,
			AppPower_Sum: 5823.13,
			PF: 0.8687,
		}*/ //opts := MQTT.NewClientOptions().AddBroker("tcp://192.168.1.103:31183")

	/* mess1,error:= json.Marshal(Message1)
	       if error != nil{
	       	fmt.Println("Json Error:", error)
		   }

			mess2,error:= json.Marshal(Message2)
			if error != nil{
				fmt.Println("Json Error:", error)
			}

			mess3,error:= json.Marshal(Message3)
			if error != nil{
				fmt.Println("Json Error:", error)
			}

			mess4,error:= json.Marshal(Message4)
			if error != nil{
				fmt.Println("Json Error:", error)
			}

			mess5,error:= json.Marshal(Message5)
			if error != nil{
				fmt.Println("Json Error:", error)
			}

			mess6,error:= json.Marshal(Message6)
			if error != nil{
				fmt.Println("Json Error:", error)
			}*/
	/*  for i:=1;i<=num;i++{
	PMUID1:=fmt.Sprintf("PMU%d", i)
	topicID1:= fmt.Sprintf("ZONE1/PMU/%d", i)
	go PublishMessage1(PMUID1,topicID1,&wg)*/
	/*	  PMUID2:=fmt.Sprintf("PMU%d", i)
		  topicID2:= fmt.Sprintf("ZONE2/PMU/%d", i)
		  go PublishMessage2(PMUID2,topicID2,&wg)

		  PMUID3:=fmt.Sprintf("PMU%d", i)
		  topicID3:= fmt.Sprintf("ZONE3/PMU/%d", i)
		  go PublishMessage3(PMUID3,topicID3,&wg)*/
	go PublishMessage1(mqttaddress, "PMU1", "ZONE1/PMU/1", &wg)
	go PublishMessage2(mqttaddress, "PMU2", "ZONE2/PMU/2", &wg)
	go PublishMessage3(mqttaddress, "PMU3", "ZONE3/PMU/3", &wg)
	// go PublishMessage4(mqttaddress,"PMU4","ZONE2/PMU/4",&wg)
	// go PublishMessage5(mqttaddress,"PMU5","ZONE3/PMU/5",&wg)
	// go PublishMessage6(mqttaddress,"PMU6","ZONE3/PMU/6",&wg)

	/*go PublishMessage1(MVA1,PVA1,MVB1,PVB1,MVC1,PVC1,MIA1,PIA1,MIB1,PIB1,MIC1,PIC1,"PMU7","ZONE1/PMU/7",&wg)
	go PublishMessage2(MVA2,PVA2,MVB2,PVB2,MVC2,PVC2,MIA2,PIA2,MIB2,PIB2,MIC2,PIC2,"PMU8","ZONE1/PMU/8",&wg)*/
	/*go PublishMessage3(MVA3,PVA3,MVB3,PVB3,MVC3,PVC3,MIA3,PIA3,MIB3,PIB3,MIC3,PIC3,"PMU9","ZONE2/PMU/9",&wg)
	go PublishMessage4(MVA4,PVA4,MVB4,PVB4,MVC4,PVC4,MIA4,PIA4,MIB4,PIB4,MIC4,PIC4,"PMU10","ZONE2/PMU/10",&wg)
	go PublishMessage5(MVA5,PVA5,MVB5,PVB5,MVC5,PVC5,MIA5,PIA5,MIB5,PIB5,MIC5,PIC5,"PMU11","ZONE3/PMU/11",&wg)
	go PublishMessage6(MVA6,PVA6,MVB6,PVB6,MVC6,PVC6,MIA6,PIA6,MIB6,PIB6,MIC6,PIC6,"PMU12","ZONE3/PMU/12",&wg)*/
	wg.Wait()
	// fmt.Println(string(mess))
}

func PublishMessage1(mqttaddress, client, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	//创建连接
	MVA1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 6)
	PVA1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 7)
	MVB1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 10)
	PVB1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 11)
	MVC1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 14)
	PVC1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 15)
	MIA1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 8)
	PIA1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 9)
	MIB1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 12)
	PIB1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 13)
	MIC1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 16)
	PIC1 := LoadData("2019-03-23_00h_UTC_PMUID01.csv", 17)

	/*		opts:= MQTT.NewClientOptions().AddBroker("tcp://192.168.1.103:31183")
			//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
			opts.SetClientID(client)		//opts.SetDefaultPublishHandler(f)
			opts.SetConnectTimeout(time.Duration(60) * time.Second)
			c := MQTT.NewClient(opts)

			if token := c.Connect(); token.WaitTimeout(time.Duration(60)*time.Second) && token.Wait() && token.Error() != nil {
				fmt.Printf("error:%s \n", token.Error())x'
				return
			}*/
	num := 2
	var opts1 [2]*MQTT.ClientOptions
	var c1 [2]MQTT.Client
	var ClientID [2]string
	var topicID [2]string
	for j := 0; j < num; j++ {
		opts1[j] = MQTT.NewClientOptions().AddBroker(mqttaddress)
		//opts1[j] = MQTT.NewClientOptions().AddBroker("tcp://192.168.1.103:31183")
		//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
		ClientID[j] = fmt.Sprintf("PMU1%d", j)
		topicID[j] = fmt.Sprintf("ZONE1/PMU/1%d", j)
		opts1[j].SetClientID(ClientID[j])
		//opts.SetDefaultPublishHandler(f)
		opts1[j].SetConnectTimeout(time.Duration(60) * time.Second)
		c1[j] = MQTT.NewClient(opts1[j])
		if token := c1[j].Connect(); token.WaitTimeout(time.Duration(60)*time.Second) && token.Wait() && token.Error() != nil {
			fmt.Printf("error:%s \n", token.Error())
			return
		}

	}

	for i := 0; i < 65535; i++ {

		mva1, _ := strconv.ParseFloat(MVA1[i], 64)
		pva1, _ := strconv.ParseFloat(PVA1[i], 64)
		mvb1, _ := strconv.ParseFloat(MVB1[i], 64)
		pvb1, _ := strconv.ParseFloat(PVB1[i], 64)
		mvc1, _ := strconv.ParseFloat(MVC1[i], 64)
		pvc1, _ := strconv.ParseFloat(PVC1[i], 64)
		mia1, _ := strconv.ParseFloat(MIA1[i], 64)
		pia1, _ := strconv.ParseFloat(PIA1[i], 64)
		mib1, _ := strconv.ParseFloat(MIB1[i], 64)
		pib1, _ := strconv.ParseFloat(PIB1[i], 64)
		mic1, _ := strconv.ParseFloat(MIC1[i], 64)
		pic1, _ := strconv.ParseFloat(PIC1[i], 64)

		Message1 := message{
			PMU_ID:    1,
			Timestamp: time.Now().UnixNano() / 1e6,
			Mag_VA1:   mva1,
			Phase_VA1: pva1,
			Mag_VB1:   mvb1,
			Phase_VB1: pvb1,
			Mag_VC1:   mvc1,
			Phase_VC1: pvc1,
			Mag_IA1:   mia1,
			Phase_IA1: pia1,
			Mag_IB1:   mib1,
			Phase_IB1: pib1,
			Mag_IC1:   mic1,
			Phase_IC1: pic1,
		}

		/*		mess1,error:= json.Marshal(Message1)
				if error != nil{
					fmt.Println("Json Error:", error)
				}*/
		/*		if token := c.Publish(topic, 1, false, mess1); token.Wait() && token.Error() != nil {
				fmt.Println(token.Error())
				os.Exit(1)
			}*/
		// go Publish(i,client,c, topic,mess1)
		for j := 0; j < num; j++ {

			Message1.PMU_ID = 10 + j
			mess1, error := json.Marshal(Message1)
			if error != nil {
				fmt.Println("Json Error:", error)
			}

			go Publish(i, ClientID[j], c1[j], topicID[j], mess1)
			/*			go Publish(i, "PMU11", c2, "ZONE1/PMU/11" , mess1)
						go Publish(i, "PMU12", c3, "ZONE1/PMU/12", mess1)
						go Publish(i, "PMU13", c4, "ZONE1/PMU/13", mess1)
						go Publish(i, "PMU14", c5, "ZONE1/PMU/14", mess1)
						go Publish(i, "PMU15", c6, "ZONE1/PMU/15", mess1)*/
		}
		/*go Publish(i,"PMU7",c, "ZONE1/PMU/7",mess1)
		  go Publish(i,"PMU8",c, "ZONE1/PMU/8",mess1)*/
		/*		go Publish(i,"PMU9",c, "ZONE1/PMU/9",mess1)
				go Publish(i,"PMU10",c, "ZONE1/PMU/10",mess1)*/
		/*		go Publish(i,"PMU11",c, "ZONE1/PMU/11",mess1)
				go Publish(i,"PMU12",c, "ZONE1/PMU/12",mess1)*/
		//token.Wait()
		time.Sleep(20 * time.Millisecond)
	}

	for j := 0; j < num; j++ {
		c1[j].Disconnect(250)
		/*			go Publish(i, "PMU11", c2, "ZONE1/PMU/11" , mess1)
					go Publish(i, "PMU12", c3, "ZONE1/PMU/12", mess1)
					go Publish(i, "PMU13", c4, "ZONE1/PMU/13", mess1)
					go Publish(i, "PMU14", c5, "ZONE1/PMU/14", mess1)
					go Publish(i, "PMU15", c6, "ZONE1/PMU/15", mess1)*/
	}

}

func Publish(i int, clinet string, c MQTT.Client, topic string, mess []byte) {
	t1 := time.Now()
	if token := c.Publish(topic, 0, false, mess); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
	fmt.Printf("Task ID %d Client: %s ", i, clinet)
	fmt.Println("Lantency:", time.Since(t1))
}

func PublishMessage2(mqttaddress, client, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	//opts := MQTT.NewClientOptions().AddBroker("tcp://192.168.0.127:1883")
	opts := MQTT.NewClientOptions().AddBroker(mqttaddress)
	//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
	opts.SetClientID(client)
	//opts.SetDefaultPublishHandler(f)
	opts.SetConnectTimeout(time.Duration(60) * time.Second)
	//创建连接
	c := MQTT.NewClient(opts)
	MVA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 6)
	PVA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 7)
	MVB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 10)
	PVB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 11)
	MVC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 14)
	PVC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 15)
	MIA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 18)
	PIA2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 19)
	MIB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 20)
	PIB2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 21)
	MIC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 22)
	PIC2 := LoadData("2019-03-23_00h_UTC_PMUID02.csv", 23)

	if token := c.Connect(); token.WaitTimeout(time.Duration(60)*time.Second) && token.Wait() && token.Error() != nil {
		fmt.Printf("error:%s \n", token.Error())
		return
	}
	num := 2
	var opts1 [2]*MQTT.ClientOptions
	var c1 [2]MQTT.Client
	var ClientID [2]string
	var topicID [2]string
	for j := 0; j < num; j++ {
		//opts1[j] = MQTT.NewClientOptions().AddBroker("tcp://192.168.0.127:1883")
		opts1[j] = MQTT.NewClientOptions().AddBroker(mqttaddress)
		//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
		ClientID[j] = fmt.Sprintf("PMU2%d", j)
		topicID[j] = fmt.Sprintf("ZONE2/PMU/2%d", j)
		opts1[j].SetClientID(ClientID[j])
		//opts.SetDefaultPublishHandler(f)
		opts1[j].SetConnectTimeout(time.Duration(60) * time.Second)
		c1[j] = MQTT.NewClient(opts1[j])
		if token := c1[j].Connect(); token.WaitTimeout(time.Duration(60)*time.Second) && token.Wait() && token.Error() != nil {
			fmt.Printf("error:%s \n", token.Error())
			return
		}

	}

	//token := c.Publish("test", 1, false, text)

	//每隔5秒向topic发送一条消息
	//time.Sleep(1 * time.Second)
	//text := fmt.Sprintf("this is msg #%d! from task:%d", i, taskId)

	for i := 0; i < 65535; i++ {

		mva2, _ := strconv.ParseFloat(MVA2[i], 64)
		pva2, _ := strconv.ParseFloat(PVA2[i], 64)
		mvb2, _ := strconv.ParseFloat(MVB2[i], 64)
		pvb2, _ := strconv.ParseFloat(PVB2[i], 64)
		mvc2, _ := strconv.ParseFloat(MVC2[i], 64)
		pvc2, _ := strconv.ParseFloat(PVC2[i], 64)
		mia2, _ := strconv.ParseFloat(MIA2[i], 64)
		pia2, _ := strconv.ParseFloat(PIA2[i], 64)
		mib2, _ := strconv.ParseFloat(MIB2[i], 64)
		pib2, _ := strconv.ParseFloat(PIB2[i], 64)
		mic2, _ := strconv.ParseFloat(MIC2[i], 64)
		pic2, _ := strconv.ParseFloat(PIC2[i], 64)

		Message2 := message{
			PMU_ID:    2,
			Timestamp: time.Now().UnixNano() / 1e6,
			Mag_VA1:   mva2,
			Phase_VA1: pva2,
			Mag_VB1:   mvb2,
			Phase_VB1: pvb2,
			Mag_VC1:   mvc2,
			Phase_VC1: pvc2,
			Mag_IA1:   mia2,
			Phase_IA1: pia2,
			Mag_IB1:   mib2,
			Phase_IB1: pib2,
			Mag_IC1:   mic2,
			Phase_IC1: pic2,
		}

		/*		mess2,error:= json.Marshal(Message2)
						if error != nil{
							fmt.Println("Json Error:", error)
						}

				/*		if token := c.Publish(topic, 1, false, mess2); token.Wait() && token.Error() != nil {
							fmt.Println(token.Error())
							os.Exit(1)
						}*/
		//go Publish(i,client,c, topic,mess2)

		for j := 0; j < num; j++ {

			Message2.PMU_ID = j + 20

			mess2, error := json.Marshal(Message2)
			if error != nil {
				fmt.Println("Json Error:", error)
			}

			go Publish(i, ClientID[j], c1[j], topicID[j], mess2)
			/*			go Publish(i, "PMU11", c2, "ZONE1/PMU/11", mess1)
						go Publish(i, "PMU12", c3, "ZONE1/PMU/12", mess1)
						go Publish(i, "PMU13", c4, "ZONE1/PMU/13", mess1)
						go Publish(i, "PMU14", c5, "ZONE1/PMU/14", mess1)
						go Publish(i, "PMU15", c6, "ZONE1/PMU/15", mess1)*/
		}

		/*		go Publish(i,"PMU20",c, "ZONE2/PMU/20",mess2)
				go Publish(i,"PMU21",c, "ZONE2/PMU/21",mess2)*/
		//token.Wait()

		time.Sleep(20 * time.Millisecond)
	}
	c.Disconnect(250)
}

func PublishMessage3(mqttaddress, client, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	//opts := MQTT.NewClientOptions().AddBroker("tcp://192.168.0.127:1883")
	opts := MQTT.NewClientOptions().AddBroker(mqttaddress)
	//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
	opts.SetClientID(client)
	//opts.SetDefaultPublishHandler(f)
	opts.SetConnectTimeout(time.Duration(60) * time.Second)
	//创建连接
	c := MQTT.NewClient(opts)
	MVA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 6)
	PVA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 7)
	MVB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 10)
	PVB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 11)
	MVC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 14)
	PVC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 15)
	MIA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 8)
	PIA3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 9)
	MIB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 12)
	PIB3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 13)
	MIC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 16)
	PIC3 := LoadData("2019-03-23_00h_UTC_PMUID03.csv", 17)

	if token := c.Connect(); token.WaitTimeout(time.Duration(60)*time.Second) && token.Wait() && token.Error() != nil {
		fmt.Printf("error:%s \n", token.Error())
		return
	}

	//token := c.Publish("test", 1, false, text)
	var num = 2
	var opts1 [2]*MQTT.ClientOptions
	var c1 [2]MQTT.Client
	var ClientID [2]string
	var topicID [2]string
	for j := 0; j < num; j++ {
		opts1[j] = MQTT.NewClientOptions().AddBroker(mqttaddress)
		//opts1[j] = MQTT.NewClientOptions().AddBroker("tcp://192.168.1.103:31183")
		//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
		ClientID[j] = fmt.Sprintf("PMU3%d", j)
		topicID[j] = fmt.Sprintf("ZONE3/PMU/3%d", j)
		opts1[j].SetClientID(ClientID[j])
		//opts.SetDefaultPublishHandler(f)
		opts1[j].SetConnectTimeout(time.Duration(60) * time.Second)
		c1[j] = MQTT.NewClient(opts1[j])
		if token := c1[j].Connect(); token.WaitTimeout(time.Duration(60)*time.Second) && token.Wait() && token.Error() != nil {
			fmt.Printf("error:%s \n", token.Error())
			return
		}

	}
	//每隔5秒向topic发送一条消息
	//time.Sleep(1 * time.Second)
	//text := fmt.Sprintf("this is msg #%d! from task:%d", i, taskId)

	for i := 0; i < 65535; i++ {

		mva3, _ := strconv.ParseFloat(MVA3[i], 64)
		pva3, _ := strconv.ParseFloat(PVA3[i], 64)
		mvb3, _ := strconv.ParseFloat(MVB3[i], 64)
		pvb3, _ := strconv.ParseFloat(PVB3[i], 64)
		mvc3, _ := strconv.ParseFloat(MVC3[i], 64)
		pvc3, _ := strconv.ParseFloat(PVC3[i], 64)
		mia3, _ := strconv.ParseFloat(MIA3[i], 64)
		pia3, _ := strconv.ParseFloat(PIA3[i], 64)
		mib3, _ := strconv.ParseFloat(MIB3[i], 64)
		pib3, _ := strconv.ParseFloat(PIB3[i], 64)
		mic3, _ := strconv.ParseFloat(MIC3[i], 64)
		pic3, _ := strconv.ParseFloat(PIC3[i], 64)

		Message3 := message{
			PMU_ID:    3,
			Timestamp: time.Now().UnixNano() / 1e6,
			Mag_VA1:   mva3,
			Phase_VA1: pva3,
			Mag_VB1:   mvb3,
			Phase_VB1: pvb3,
			Mag_VC1:   mvc3,
			Phase_VC1: pvc3,
			Mag_IA1:   mia3,
			Phase_IA1: pia3,
			Mag_IB1:   mib3,
			Phase_IB1: pib3,
			Mag_IC1:   mic3,
			Phase_IC1: pic3,
		}

		/*		if token := c.Publish(topic, 1, false, mess); token.Wait() && token.Error() != nil {
				fmt.Println(token.Error())
				os.Exit(1)
			}*/

		//go Publish(i,client,c, topic,mess)
		for j := 0; j < num; j++ {
			Message3.PMU_ID = j + 30
			mess, error := json.Marshal(Message3)
			if error != nil {
				fmt.Println("Json Error:", error)
			}

			go Publish(i, ClientID[j], c1[j], topicID[j], mess)
			/*			go Publish(i, "PMU11", c2, "ZONE1/PMU/11", mess1)
						go Publis/home/hadooph(i, "PMU12", c3, "ZONE1/PMU/12", mess1)
						go Publish(i, "PMU13", c4, "ZONE1/PMU/13", mess1)
						go Publish(i, "PMU14", c5, "ZONE1/PMU/14", mess1)
						go Publish(i, "PMU15", c6, "ZONE1/PMU/15", mess1)*/
		}
		//token.Wait()

		time.Sleep(20 * time.Millisecond)
	}
	c.Disconnect(250)
}

func PublishMessage4(mqttaddress, client, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	opts := MQTT.NewClientOptions().AddBroker(mqttaddress)
	//opts := MQTT.NewClientOptions().AddBroker("tcp://192.168.0.127:1883")
	//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
	opts.SetClientID(client)
	//opts.SetDefaultPublishHandler(f)
	opts.SetConnectTimeout(time.Duration(60) * time.Second)
	//创建连接
	c := MQTT.NewClient(opts)
	MVA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 6)
	PVA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 7)
	MVB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 10)
	PVB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 11)
	MVC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 14)
	PVC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 15)
	MIA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 8)
	PIA4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 9)
	MIB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 12)
	PIB4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 13)
	MIC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 16)
	PIC4 := LoadData("2019-03-23_00h_UTC_PMUID04.csv", 17)

	if token := c.Connect(); token.WaitTimeout(time.Duration(60)*time.Second) && token.Wait() && token.Error() != nil {
		fmt.Printf("error:%s \n", token.Error())
		return
	}
	//token := c.Publish("test", 1, false, text)

	//每隔5秒向topic发送一条消息
	//time.Sleep(1 * time.Second)
	//text := fmt.Sprintf("this is msg #%d! from task:%d", i, taskId)

	for i := 0; i < 65535; i++ {

		mva4, _ := strconv.ParseFloat(MVA4[i], 64)
		pva4, _ := strconv.ParseFloat(PVA4[i], 64)
		mvb4, _ := strconv.ParseFloat(MVB4[i], 64)
		pvb4, _ := strconv.ParseFloat(PVB4[i], 64)
		mvc4, _ := strconv.ParseFloat(MVC4[i], 64)
		pvc4, _ := strconv.ParseFloat(PVC4[i], 64)
		mia4, _ := strconv.ParseFloat(MIA4[i], 64)
		pia4, _ := strconv.ParseFloat(PIA4[i], 64)
		mib4, _ := strconv.ParseFloat(MIB4[i], 64)
		pib4, _ := strconv.ParseFloat(PIB4[i], 64)
		mic4, _ := strconv.ParseFloat(MIC4[i], 64)
		pic4, _ := strconv.ParseFloat(PIC4[i], 64)

		Message4 := message{
			PMU_ID:    4,
			Timestamp: time.Now().UnixNano() / 1e6,
			Mag_VA1:   mva4,
			Phase_VA1: pva4,
			Mag_VB1:   mvb4,
			Phase_VB1: pvb4,
			Mag_VC1:   mvc4,
			Phase_VC1: pvc4,
			Mag_IA1:   mia4,
			Phase_IA1: pia4,
			Mag_IB1:   mib4,
			Phase_IB1: pib4,
			Mag_IC1:   mic4,
			Phase_IC1: pic4,
		}

		mess, error := json.Marshal(Message4)
		if error != nil {
			fmt.Println("Json Error:", error)
		}

		/*		if token := c.Publish(topic, 1, false, mess); token.Wait() && token.Error() != nil {
				fmt.Println(token.Error())
				os.Exit(1)
			}*/
		go Publish(i, client, c, topic, mess)
		//token.Wait()

		time.Sleep(20 * time.Millisecond)
	}
	c.Disconnect(250)
}

func PublishMessage5(mqttaddress, client, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	//opts := MQTT.NewClientOptions().AddBroker("tcp://192.168.0.127:1883")
	opts := MQTT.NewClientOptions().AddBroker(mqttaddress)
	//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
	opts.SetClientID(client)
	//opts.SetDefaultPublishHandler(f)
	opts.SetConnectTimeout(time.Duration(60) * time.Second)
	//创建连接
	c := MQTT.NewClient(opts)
	MVA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 6)
	PVA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 7)
	MVB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 10)
	PVB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 11)
	MVC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 14)
	PVC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 15)
	MIA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 8)
	PIA5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 9)
	MIB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 12)
	PIB5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 13)
	MIC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 16)
	PIC5 := LoadData("2019-03-23_00h_UTC_PMUID05.csv", 17)

	if token := c.Connect(); token.WaitTimeout(time.Duration(60)*time.Second) && token.Wait() && token.Error() != nil {
		fmt.Printf("error:%s \n", token.Error())
		return
	}
	//token := c.Publish("test", 1, false, text)

	//每隔5秒向topic发送一条消息
	//time.Sleep(1 * time.Second)
	//text := fmt.Sprintf("this is msg #%d! from task:%d", i, taskId)

	for i := 0; i < 65535; i++ {

		mva5, _ := strconv.ParseFloat(MVA5[i], 64)
		pva5, _ := strconv.ParseFloat(PVA5[i], 64)
		mvb5, _ := strconv.ParseFloat(MVB5[i], 64)
		pvb5, _ := strconv.ParseFloat(PVB5[i], 64)
		mvc5, _ := strconv.ParseFloat(MVC5[i], 64)
		pvc5, _ := strconv.ParseFloat(PVC5[i], 64)
		mia5, _ := strconv.ParseFloat(MIA5[i], 64)
		pia5, _ := strconv.ParseFloat(PIA5[i], 64)
		mib5, _ := strconv.ParseFloat(MIB5[i], 64)
		pib5, _ := strconv.ParseFloat(PIB5[i], 64)
		mic5, _ := strconv.ParseFloat(MIC5[i], 64)
		pic5, _ := strconv.ParseFloat(PIC5[i], 64)

		Message5 := message{
			PMU_ID:    5,
			Timestamp: time.Now().UnixNano() / 1e6,
			Mag_VA1:   mva5,
			Phase_VA1: pva5,
			Mag_VB1:   mvb5,
			Phase_VB1: pvb5,
			Mag_VC1:   mvc5,
			Phase_VC1: pvc5,
			Mag_IA1:   mia5,
			Phase_IA1: pia5,
			Mag_IB1:   mib5,
			Phase_IB1: pib5,
			Mag_IC1:   mic5,
			Phase_IC1: pic5,
		}

		mess, error := json.Marshal(Message5)
		if error != nil {
			fmt.Println("Json Error:", error)
		}

		/*	if token := c.Publish(topic, 1, false, mess); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
			os.Exit(1)
		}*/
		go Publish(i, client, c, topic, mess)
		//token.Wait()

		time.Sleep(20 * time.Millisecond)
	}
	c.Disconnect(250)
}

func PublishMessage6(mqttaddress, client, topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	//opts := MQTT.NewClientOptions().AddBroker("tcp://192.168.0.127:1883")
	opts := MQTT.NewClientOptions().AddBroker(mqttaddress)
	//opts.SetClientID(fmt.Sprintf("go-simple-client:%d", taskId))
	opts.SetClientID(client)
	//opts.SetDefaultPublishHandler(f)
	opts.SetConnectTimeout(time.Duration(60) * time.Second)
	//创建连接
	c := MQTT.NewClient(opts)
	MVA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 6)
	PVA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 7)
	MVB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 10)
	PVB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 11)
	MVC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 14)
	PVC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 15)
	MIA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 8)
	PIA6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 9)
	MIB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 12)
	PIB6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 13)
	MIC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 16)
	PIC6 := LoadData("2019-03-23_00h_UTC_PMUID06.csv", 17)

	if token := c.Connect(); token.WaitTimeout(time.Duration(60)*time.Second) && token.Wait() && token.Error() != nil {
		fmt.Printf("error:%s \n", token.Error())
		return
	}
	//token := c.Publish("test", 1, false, text)

	//每隔5秒向topic发送一条消息
	//time.Sleep(1 * time.Second)
	//text := fmt.Sprintf("this is msg #%d! from task:%d", i, taskId)

	for i := 0; i < 65535; i++ {

		mva6, _ := strconv.ParseFloat(MVA6[i], 64)
		pva6, _ := strconv.ParseFloat(PVA6[i], 64)
		mvb6, _ := strconv.ParseFloat(MVB6[i], 64)
		pvb6, _ := strconv.ParseFloat(PVB6[i], 64)
		mvc6, _ := strconv.ParseFloat(MVC6[i], 64)
		pvc6, _ := strconv.ParseFloat(PVC6[i], 64)
		mia6, _ := strconv.ParseFloat(MIA6[i], 64)
		pia6, _ := strconv.ParseFloat(PIA6[i], 64)
		mib6, _ := strconv.ParseFloat(MIB6[i], 64)
		pib6, _ := strconv.ParseFloat(PIB6[i], 64)
		mic6, _ := strconv.ParseFloat(MIC6[i], 64)
		pic6, _ := strconv.ParseFloat(PIC6[i], 64)

		Message6 := message{
			PMU_ID:    6,
			Timestamp: time.Now().UnixNano() / 1e6,
			Mag_VA1:   mva6,
			Phase_VA1: pva6,
			Mag_VB1:   mvb6,
			Phase_VB1: pvb6,
			Mag_VC1:   mvc6,
			Phase_VC1: pvc6,
			Mag_IA1:   mia6,
			Phase_IA1: pia6,
			Mag_IB1:   mib6,
			Phase_IB1: pib6,
			Mag_IC1:   mic6,
			Phase_IC1: pic6,
		}

		mess, error := json.Marshal(Message6)
		if error != nil {
			fmt.Println("Json Error:", error)
		}

		/*		if token := c.Publish(topic, 1, false, mess); token.Wait() && token.Error() != nil {
				fmt.Println(token.Error())
				os.Exit(1)
			}*/
		go Publish(i, client, c, topic, mess)
		//token.Wait()

		time.Sleep(20 * time.Millisecond)
	}
	c.Disconnect(250)
}

func LoadData(filepath string, p int) []string {
	var data = make([]string, 0)
	fs, r, err := ReadCSV(filepath)
	defer fs.Close()

	if err != nil {
		fmt.Println("文件读取错误, error", err)
	}
	for {
		row, err := r.Read()
		if err != nil && err != io.EOF {
			log.Fatalf("can not read, err is %+v", err)
		}
		if err == io.EOF {
			break
		}
		data = append(data, row[p])
	}
	return data[1:]
}

func ReadCSV(fileName string) (*os.File, *csv.Reader, error) {

	fs, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("can not open the file, err is %+v", err)
	}
	//defer fs.Close()
	r := csv.NewReader(fs)
	return fs, r, err
}
