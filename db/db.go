package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
)

type DriverName string

const (
	Vertica DriverName = "vertica"
	PG      DriverName = "postgres"
)

type Track struct {
	SnapId      string
	PeopleId    string
	DeviceId    string
	DiscardInfo string
	TrackType   int
}

func (t Track) String() string {
	return fmt.Sprintf("%s,%s,%d", t.SnapId, t.PeopleId, t.TrackType)
}

func (t Track) TableName() string {
	return "viid_facestatic.people_track"
}

type FaceInfo struct {
	FaceId           string
	DeviceId         string
	ImageUrl         string
	Passtime         int
	ImageReliability int
	Roll             float32
	Yaw              float32
	Pitch            float32
}

func (f FaceInfo) String() string {
	return fmt.Sprintf("%s,%s,%s,%d,%d,%f,%f,%f",
		f.FaceId, f.DeviceId, f.ImageUrl, f.Passtime, f.ImageReliability, f.Roll, f.Yaw, f.Pitch)
}

type PersonInfo struct {
	PersonId   string
	DeviceId   string
	ImageUrl   string
	LinkFaceId string
	Width      int
	Height     int
}

func Connect(driver DriverName, dbConnectString string) *sql.DB {
	conn, err := sql.Open(string(driver), dbConnectString)
	if err != nil {
		log.Fatalln("connect db err: ", err)
	}
	return conn
}

//检索轨迹信息
func QueryTrack(conn *sql.DB, snapIds []string) []Track {
	inStr := strings.Join(snapIds, "','")
	sql := fmt.Sprintf("select snap_id, people_id, type, device_id from viid_facestatic.people_track where snap_id in ('%s')", inStr)
	rs, err := conn.Query(sql)
	if err != nil {
		log.Fatalln("query track err: ", err)
	}
	defer rs.Close()
	var tracks []Track = make([]Track, 0)
	for rs.Next() {
		var track Track
		rs.Scan(&track.SnapId, &track.PeopleId, &track.TrackType, &track.DeviceId)
		tracks = append(tracks, track)
	}
	return tracks
}

func QueryTrash(conn *sql.DB, snapIds []string) []Track {
	inStr := strings.Join(snapIds, "','")
	sql := fmt.Sprintf("select record_id, discard_reason from viid_facestatic.trash_archive where record_id in ('%s')", inStr)
	rs, err := conn.Query(sql)
	if err != nil {
		log.Fatalln("query trash err: ", err)
	}
	var tracks []Track = make([]Track, 0)
	for rs.Next() {
		var track Track
		rs.Scan(&track.SnapId, &track.DiscardInfo)
		tracks = append(tracks, track)
	}
	return tracks
}

//检索人脸
func QueryFace(conn *sql.DB, faceIds []string) []FaceInfo {
	inStr := strings.Join(faceIds, "','")
	sqlStr := fmt.Sprintf("select faceid, deviceid, imageurlpart, passtime, imagereliability, roll, yaw, pitch from viid_facesnap.facesnapstructured_a050000 where faceid in ('%s')", inStr)
	rs, err := conn.Query(sqlStr)
	if err != nil {
		log.Fatalln("query face err: ", err)
	}
	faceInfos := make([]FaceInfo, 0)
	for rs.Next() {
		var face FaceInfo
		imageUrl := sql.NullString{}
		imageReliability := sql.NullInt32{}
		roll := sql.NullFloat64{}
		yaw := sql.NullFloat64{}
		pitch := sql.NullFloat64{}
		rs.Scan(&face.FaceId, &face.DeviceId, &imageUrl, &face.Passtime, &imageReliability, &roll, &yaw, &pitch)
		face.ImageUrl = imageUrl.String
		face.ImageReliability = int(imageReliability.Int32)
		face.Roll = float32(roll.Float64)
		face.Yaw = float32(yaw.Float64)
		face.Pitch = float32(pitch.Float64)
		faceInfos = append(faceInfos, face)
	}
	return faceInfos
}

//检索人体
func QueryPerson(conn *sql.DB, personIds []string) []PersonInfo {
	inStr := strings.Join(personIds, "','")
	sqlStr := fmt.Sprintf("select personid, deviceid, imageurlpart, linkfacepersonid, rightbtmx-lefttopx, rightbtmy-lefttopy from viid_person.personstructured_a050300 where personid in ('%s')", inStr)
	rs, err := conn.Query(sqlStr)
	if err != nil {
		log.Fatalln("query person err: ", err)
	}
	defer rs.Close()
	personInfos := make([]PersonInfo, 0)
	for rs.Next() {
		var p PersonInfo
		image := sql.NullString{String: "", Valid: false}
		linkeFaceId := sql.NullString{String: "", Valid: false}
		rs.Scan(&p.PersonId, &p.DeviceId, &image, &linkeFaceId, &p.Width, &p.Height)
		p.ImageUrl = image.String
		p.LinkFaceId = linkeFaceId.String
		personInfos = append(personInfos, p)
	}
	return personInfos
}

func QueryTask(conn *sql.DB, date string) []string {
	sqlStr := fmt.Sprintf("select work_task_id from pvid_person.person_archive_work_task where date(to_timestamp(create_time/1000)) = '%s'", date)
	log.Println("query person task for date: ", date)
	rs, err := conn.Query(sqlStr)
	if err != nil {
		log.Println("query person task info err: ", err)
		return nil
	}
	result := make([]string, 0)
	for rs.Next() {
		var id string
		rs.Scan(&id)
		result = append(result, id)
	}
	log.Println("person task to analyze: ", result)
	return result
}

func QueryPersonArchiveIds(conn *sql.DB, ids []string) []string {
	sqlStr := fmt.Sprintf("select device_id from pvid_system.device_info where device_id in ('%s') and archive_type = 2", strings.Join(ids, "','"))
	log.Println("query person archive device")
	rs, err := conn.Query(sqlStr)
	if err != nil {
		log.Println("query person archive device err: ", err)
		return nil
	}
	result := make([]string, 0)
	for rs.Next() {
		var id string
		rs.Scan(&id)
		result = append(result, id)
	}
	log.Println("person device id: ", result)
	return result
}
