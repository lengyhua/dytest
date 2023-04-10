package file

import (
	"dytest/utils"
	"encoding/json"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
	"unicode"
)

type IdType int

const (
	Face    IdType = 0
	Person  IdType = 1
	InValid IdType = -1
)

type IdStruct struct {
	Name       string
	PersonIds  []string
	FaceIds    []string
	InvalidIds []string
}

//通过id判断当前是人脸还是人体
func typeOf(id string) IdType {
	if len(strings.TrimSpace(id)) != 48 {
		return InValid
	}
	if id[41:43] == "06" {
		return Face
	}
	return Person
}

func ReadFile(f string) (IdStruct, error) {
	var idStruct IdStruct
	_, fileName := filepath.Split(f)
	idStruct.Name = fileName
	bs, err := ioutil.ReadFile(f)
	if err != nil {
		return idStruct, err
	}
	contents := string(bs)
	s := strings.FieldsFunc(contents, func(r rune) bool { return unicode.IsSpace(r) })

	for _, id := range s {
		if typeOf(id) == Face {
			idStruct.FaceIds = append(idStruct.FaceIds, id)
		} else if typeOf(id) == Person {
			idStruct.PersonIds = append(idStruct.PersonIds, id)
		} else {
			idStruct.InvalidIds = append(idStruct.InvalidIds, id)
		}
	}
	return idStruct, nil
}

func ReadDir(dir string) ([]IdStruct, error) {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	idStructs := make([]IdStruct, 0)
	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}
		is, _ := ReadFile(filepath.Join(dir, fi.Name()))
		idStructs = append(idStructs, is)
	}
	return idStructs, nil
}

const (
	NotFound           string = "未找到"
	SmallSize          string = "宽高不满足要求"
	SingleArchiveTrash string = "单档案"
	BigArchiveTrash    string = "大档案"
	NoLinkArchiveTrash string = "无关联档案"
	UnLinkArchiveTrash string = "关联人脸未入档"
	SplitArchiveTrash  string = "分裂档案"
)

type S3Result struct {
	Id          string
	BigArchives []BigArchive
	SingleArchive
	UnlinkArchives []UnlinkArchive
	NolinkArchives []NolinkArchive
	SplitArchives  []SplitArchive
}

type BigArchive struct {
	DeviceNum int      `json:"deviceNum"`
	TrackNum  int      `json:"archiveNum"`
	Devices   []string `json:"devices"`
	Tracks    []string `json:"archive"`
}

type SingleArchive []string

type UnlinkArchive struct {
	ArchiveId string   `json:"archiveId"`
	PersonIds []string `json:"personIds"`
}

type SplitArchive struct {
	PeopleSize int            `json:"peopleSize"`
	TrackNum   int            `json:"archiveNum"`
	PeopleInfo map[string]int `json:"people"`
	Tracks     []string       `json:"archive"`
}

type NolinkArchive UnlinkArchive

func (r S3Result) TrashInfo(id string) (string, interface{}) {
	if utils.IsIn(r.SingleArchive, id) {
		return SingleArchiveTrash, id
	}
	for _, bigArchive := range r.BigArchives {
		if utils.IsIn(bigArchive.Tracks, id) {
			return BigArchiveTrash, bigArchive
		}
	}
	for _, noLinkeArchive := range r.NolinkArchives {
		if utils.IsIn(noLinkeArchive.PersonIds, id) {
			return NoLinkArchiveTrash, noLinkeArchive
		}
	}
	for _, unLinkArchive := range r.UnlinkArchives {
		if utils.IsIn(unLinkArchive.PersonIds, id) {
			return UnLinkArchiveTrash, unLinkArchive
		}
	}
	for _, splitArchive := range r.SplitArchives {
		if utils.IsIn(splitArchive.Tracks, id) {
			return SplitArchiveTrash, splitArchive
		}
	}
	return NotFound, nil
}

func ReadTaskResult(root string, tasks []string) ([]S3Result, error) {
	result := make([]S3Result, 0)

	for _, workTask := range tasks {
		var r = S3Result{Id: workTask}
		bigArchivePath := filepath.Join(root, r.Id, "Archive", "Big-Archive")
		b, err := ioutil.ReadFile(bigArchivePath)
		if err != nil {
			log.Println("read big archive error: ", bigArchivePath, err)
		} else {
			json.Unmarshal(b, &r.BigArchives)
		}
		singleArchivePath := filepath.Join(root, r.Id, "Archive", "Single-Archive")
		b, err = ioutil.ReadFile(singleArchivePath)
		if err != nil {
			log.Println("read single archive error: ", singleArchivePath, err)
		} else {
			json.Unmarshal(b, &r.SingleArchive)
		}
		noLinkArchivePath := filepath.Join(root, r.Id, "Archive", "No-Linked-Archive")
		b, err = ioutil.ReadFile(noLinkArchivePath)
		if err != nil {
			log.Println("read no link archive error: ", noLinkArchivePath, err)
		} else {
			json.Unmarshal(b, &r.NolinkArchives)
		}
		splitArchivePath := filepath.Join(root, r.Id, "Archive", "Split-Archive")
		b, err = ioutil.ReadFile(splitArchivePath)
		if err != nil {
			log.Println("read split archive error: ", singleArchivePath, err)
		} else {
			json.Unmarshal(b, &r.SplitArchives)
		}
		result = append(result, r)
	}
	return result, nil
}
