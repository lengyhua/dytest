package main

import (
	"database/sql"
	"dytest/db"
	"dytest/file"
	"dytest/utils"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/vertica/vertica-sql-go"
)

//数据库连接信息

type VerticaConnInfo struct {
	host     string
	port     int
	user     string
	password string
}

type PgConnInfo struct {
	host     string
	port     int
	user     string
	password string
}

var (
	date string
	root string
	dir  string

	vconn string
	pconn string
)

type AnalyzeResult struct {
	SnapInfo      SnapInfo
	Name          string
	DeviceIds     []string
	PeopleInfos   []PeopleInfo
	FaceDiscards  []FaceDiscard
	PersonDiscard []PersonDiscard
}

func (r AnalyzeResult) Write(writer *os.File) {
	log.Println("start to write result to file: ", r.Name)
	writer.WriteString("该走点人走点基本信息如下: \n")
	writer.WriteString(fmt.Sprintf("-设备数: %d, 人脸设备: %d, 人体设备: %d\n",
		len(utils.RemoveDeplicated(append(r.SnapInfo.FaceDevices, r.SnapInfo.PersonDevices...))),
		len(r.SnapInfo.FaceDevices), len(r.SnapInfo.PersonDevices)))
	writer.WriteString(fmt.Sprintf("-人脸抓拍数: %d\n", r.SnapInfo.FaceSnapNum))
	writer.WriteString(fmt.Sprintf("-人体抓拍数: %d\n", r.SnapInfo.PersonSnapNum))

	writer.WriteString("聚档信息如下: \n")
	writer.WriteString(fmt.Sprintf("-召回设备数: %d\n", len(r.DeviceIds)))
	writer.WriteString(fmt.Sprintf("-召回设备列表: %s\n", strings.Join(r.DeviceIds, ",")))
	writer.WriteString(fmt.Sprintf("-档案数: %d\n", len(r.PeopleInfos)))
	writer.WriteString("-档案详情: \n")
	for _, p := range r.PeopleInfos {
		writer.WriteString("-------------------------------------\n")
		p.Write(writer)
	}

	writer.WriteString("-------------------------------------\n")
	writer.WriteString("-人脸丢弃信息: \n")
	for _, f := range r.FaceDiscards {
		f.Write(writer)
	}

	writer.WriteString("-人体丢弃信息: \n")
	for _, p := range r.PersonDiscard {
		writer.WriteString("-------------------------------------\n")
		p.Write(writer)
	}
	log.Println("end write result: ", r.Name)
}

type SnapInfo struct {
	FaceSnapNum   int
	PersonSnapNum int
	FaceDevices   []string
	PersonDevices []string
}

type PeopleInfo struct {
	PeopleId     string
	DeviceIds    []string
	PersonTracks []string
	PersonDevice []string
	FaceTracks   []string
	FaceDevice   []string
}

func (p PeopleInfo) Write(writer *os.File) {
	writer.WriteString(fmt.Sprintf("|档案ID: %s\n", p.PeopleId))
	writer.WriteString(fmt.Sprintf("|设备数: %d, 人脸设备数: %d, 人体设备数: %d\n", len(p.DeviceIds), len(p.FaceDevice), len(p.PersonDevice)))
	writer.WriteString(fmt.Sprintf("|人脸抓拍数: %d, 人体抓拍数: %d\n", len(p.FaceTracks), len(p.PersonTracks)))
	writer.WriteString(fmt.Sprintf("|设备列表: %s\n", strings.Join(p.DeviceIds, ",")))
	writer.WriteString(fmt.Sprintf("|人脸设备列表: %s\n", strings.Join(p.FaceDevice, ",")))
	writer.WriteString(fmt.Sprintf("|人体设备列表: %s\n", strings.Join(p.PersonDevice, ",")))
}

type FaceDiscard struct {
	DiscardReason string
	Ids           []string
}

func (f FaceDiscard) Write(writer *os.File) {
	writer.WriteString(fmt.Sprintf("|丢弃原因: %s, 数量: %d\n", f.DiscardReason, len(f.Ids)))
	writer.WriteString(fmt.Sprintf("|丢弃抓拍: %s\n", strings.Join(f.Ids, ",")))
}

type PersonDiscard struct {
	Id                string
	DiscardReason     string
	WorkTask          string
	PersonArchiveInfo interface{}
}

func (p PersonDiscard) Write(writer *os.File) {
	writer.WriteString(fmt.Sprintf("|任务: %s, 丢弃原因: %s, 人体抓拍: %s\n", p.WorkTask, p.DiscardReason, p.Id))
	writer.WriteString(fmt.Sprintf("|详情: %v\n", p.PersonArchiveInfo))
}

func (r *AnalyzeResult) clean() {
	r.DeviceIds = utils.RemoveDeplicated(r.DeviceIds)
	for i := 0; i < len(r.PeopleInfos); i++ {
		r.PeopleInfos[i].DeviceIds = utils.RemoveDeplicated(r.PeopleInfos[i].DeviceIds)
		r.PeopleInfos[i].FaceDevice = utils.RemoveDeplicated(r.PeopleInfos[i].FaceDevice)
		r.PeopleInfos[i].PersonDevice = utils.RemoveDeplicated(r.PeopleInfos[i].PersonDevice)
	}
	r.SnapInfo.FaceDevices = utils.RemoveDeplicated(r.SnapInfo.FaceDevices)
	r.SnapInfo.PersonDevices = utils.RemoveDeplicated(r.SnapInfo.PersonDevices)
}

func (r *AnalyzeResult) personTrackIds() []string {
	var ids []string
	for _, p := range r.PeopleInfos {
		ids = append(ids, p.PersonTracks...)
	}
	return ids
}

func analyze(conn *sql.DB, idStruct file.IdStruct) AnalyzeResult {
	log.Println("start to process: ", idStruct.Name)
	result := AnalyzeResult{Name: idStruct.Name}
	processSnapInfo(conn, idStruct, &result)
	processTracks(conn, idStruct, &result)
	processFaceTrash(conn, idStruct, &result)
	processPersonTrash(idStruct, &result, conn)
	result.clean()
	return result
}

func processSnapInfo(conn *sql.DB, idStruct file.IdStruct, result *AnalyzeResult) {
	result.SnapInfo.FaceSnapNum = len(idStruct.FaceIds)
	result.SnapInfo.PersonSnapNum = len(idStruct.PersonIds)
	log.Println("process snap info, snap face num:", result.SnapInfo.FaceSnapNum, " snap person num: ", result.SnapInfo.PersonSnapNum)
	fis := db.QueryFace(conn, idStruct.FaceIds)
	for _, fi := range fis {
		result.SnapInfo.FaceDevices = append(result.SnapInfo.FaceDevices, fi.DeviceId)
	}
	pis := db.QueryPerson(conn, idStruct.PersonIds)
	for _, pi := range pis {
		result.SnapInfo.PersonDevices = append(result.SnapInfo.PersonDevices, pi.DeviceId)
	}
}

func processPersonTrash(idStruct file.IdStruct, result *AnalyzeResult, conn *sql.DB) {
	log.Println("start to process person trash")
	personTrashIds := utils.Substract(idStruct.PersonIds, result.personTrackIds())
	personDiscardMap := make(map[string]PersonDiscard)
	pis := db.QueryPerson(conn, personTrashIds)
	for _, pi := range pis {
		personDiscard := PersonDiscard{Id: pi.PersonId}
		if pi.Height < 150 || pi.Width < 60 {
			personDiscard.DiscardReason = file.SmallSize
		}
		personDiscardMap[pi.PersonId] = personDiscard
	}
	d := db.Connect(db.PG, pconn)
	tasks := db.QueryTask(d, date)
	s3Results, err := file.ReadTaskResult(root, tasks)
	if err == nil {
		for k, v := range personDiscardMap {
			if v.DiscardReason == "" {
				for _, r := range s3Results {
					discardReason, info := r.TrashInfo(v.Id)
					if discardReason != file.NotFound {
						v.DiscardReason = discardReason
						v.WorkTask = r.Id
						v.PersonArchiveInfo = info
						personDiscardMap[k] = v
						break
					}
				}
			}
		}
	}
	for _, d := range personDiscardMap {
		result.PersonDiscard = append(result.PersonDiscard, d)
	}
}

func processFaceTrash(conn *sql.DB, idStruct file.IdStruct, result *AnalyzeResult) {
	log.Println("start to process face trash")
	trashes := db.QueryTrash(conn, idStruct.FaceIds)
	trashMap := make(map[string][]db.Track)
	for _, t := range trashes {
		trashMap[t.DiscardInfo] = append(trashMap[t.DiscardInfo], t)
	}

	for k, v := range trashMap {
		faceDiscard := FaceDiscard{DiscardReason: k}
		for _, t := range v {
			faceDiscard.Ids = append(faceDiscard.Ids, t.SnapId)
		}
		result.FaceDiscards = append(result.FaceDiscards, faceDiscard)
	}
}

func processTracks(conn *sql.DB, idStruct file.IdStruct, result *AnalyzeResult) {
	log.Println("start to process tracks")
	tracks := db.QueryTrack(conn, append(idStruct.FaceIds, idStruct.PersonIds...))
	trackMap := make(map[string][]db.Track)
	for _, t := range tracks {
		trackMap[t.PeopleId] = append(trackMap[t.PeopleId], t)
	}
	for k, v := range trackMap {
		people := PeopleInfo{PeopleId: k}
		for _, t := range v {
			result.DeviceIds = append(result.DeviceIds, t.DeviceId)
			people.DeviceIds = append(people.DeviceIds, t.DeviceId)
			if t.TrackType == 0 {
				people.FaceDevice = append(people.FaceDevice, t.DeviceId)
				people.FaceTracks = append(people.FaceTracks, t.SnapId)
			} else {
				people.PersonDevice = append(people.PersonDevice, t.DeviceId)
				people.FaceTracks = append(people.FaceTracks, t.SnapId)
			}
		}
		result.DeviceIds = utils.RemoveDeplicated(result.DeviceIds)
		result.PeopleInfos = append(result.PeopleInfos, people)
	}
}

//解析命令行参数
func parseArgs() {
	var pgConnInfo PgConnInfo
	var verticaConnInfo VerticaConnInfo
	flag.StringVar(&verticaConnInfo.user, "u", "dbadmin", "MPP数据库用户名")
	flag.StringVar(&verticaConnInfo.password, "a", "passwd", "MPP数据库密码")
	flag.IntVar(&verticaConnInfo.port, "p", 5433, "MPP数据库端口(5433)")
	flag.StringVar(&verticaConnInfo.host, "h", "152.9.10.34", "MPP数据库服务IP")

	flag.StringVar(&pgConnInfo.user, "U", "pgsql", "PG数据库用户名")
	flag.StringVar(&pgConnInfo.password, "A", "pgsql", "PG数据库密码")
	flag.IntVar(&pgConnInfo.port, "P", 31583, "PG数据库端口(31583)")
	flag.StringVar(&pgConnInfo.host, "H", "152.9.11.99", "PG数据库服务IP")

	flag.StringVar(&date, "t", "", "要分析的人体聚档任务时间(yyyy-MM-dd)")
	flag.StringVar(&root, "s", "/home/minio/data/pvid/person", "S3根目录")
	flag.StringVar(&dir, "d", "data", "要分析数据所在目录")

	flag.Parse()

	if date == "" {
		date = time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	}

	vconn = fmt.Sprintf("vertica://%s:%s@%s:%d/viid?sslmode=disable",
		verticaConnInfo.user, verticaConnInfo.password, verticaConnInfo.host, verticaConnInfo.port)
	log.Println("vertica conntion info: ", vconn)
	pconn = fmt.Sprintf("postgres://%s:%s@%s:%d/pvid?sslmode=disable",
		pgConnInfo.user, pgConnInfo.password, pgConnInfo.host, pgConnInfo.port)
	log.Println("pg connection info: ", pconn)
}

func main() {
	parseArgs()
	conn := db.Connect(db.Vertica, vconn)
	defer conn.Close()
	is, err := file.ReadDir(dir)
	if err != nil {
		log.Fatalln("read dir err: ", dir)
	}
	for _, i := range is {
		ar := analyze(conn, i)
		resultPath := filepath.Join(dir, "result")
		os.MkdirAll(resultPath, 0777)
		os.Create(filepath.Join(resultPath, ar.Name))
		f, err := os.OpenFile(filepath.Join(resultPath, ar.Name), os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalln(err)
		}
		ar.Write(f)
	}
}
