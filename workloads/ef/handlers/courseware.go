package handlers

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	"tfaas/golang/types"

	"github.com/lechou-0/Client/CacheClient"
)

type students struct {
	StudentList []string `json:"students"`
}

type courses struct {
	CourseList []string `json:"courses"`
}

type enrollments struct {
	NumLimit    int      `json:"limit"`
	StudentList []string `json:"students"`
}

type student struct {
	CourseList []string `json:"courses"`
}

type RegisterHandler struct {
	env             types.Environment
	connectionCache *CacheClient.ConnectionCache
}

func NewRegisterHandler(env types.Environment, connectionCache *CacheClient.ConnectionCache) types.FuncHandler {
	return &RegisterHandler{
		env:             env,
		connectionCache: connectionCache,
	}
}

func (h *RegisterHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	output := map[string]any{}
	cacheClient, params, err := CacheClient.NewClient(input, h.connectionCache)
	if err != nil {
		log.Println(err)
		output["error"] = " " + err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	studentId := params["sid"].(string)
	courseId := params["cid"].(string)
	es := &enrollments{}
	stu := &student{}
	err = json.Unmarshal(cacheClient.Get(courseId), es)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	err = json.Unmarshal(cacheClient.Get(studentId), stu)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	for _, cid := range stu.CourseList {
		if cid == courseId {
			output["success"] = true
			return CacheClient.DeleteClient(cacheClient, output), nil
		}
	}
	es.StudentList = append(es.StudentList, studentId)
	newes, err := json.Marshal(es)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	err = cacheClient.Set(courseId, newes)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	stu.CourseList = append(stu.CourseList, courseId)
	newstu, err := json.Marshal(stu)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	err = cacheClient.Set(studentId, newstu)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	output["success"] = true
	return CacheClient.DeleteClient(cacheClient, output), nil
}

type DeleteHandler struct {
	env             types.Environment
	connectionCache *CacheClient.ConnectionCache
}

func NewDeleteHandler(env types.Environment, connectionCache *CacheClient.ConnectionCache) types.FuncHandler {
	return &DeleteHandler{
		env:             env,
		connectionCache: connectionCache,
	}
}

func (h *DeleteHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	output := map[string]any{}
	cacheClient, params, err := CacheClient.NewClient(input, h.connectionCache)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	courseId := params["cid"].(string)
	cs := &courses{}
	err = json.Unmarshal(cacheClient.Get("courses"), cs)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	for i, cid := range cs.CourseList {
		if cid == courseId {
			cs.CourseList = append(cs.CourseList[:i], cs.CourseList[i+1:]...)
			newcs, err := json.Marshal(cs)
			if err != nil {
				log.Println(err)
				output["error"] = err.Error()
				return CacheClient.DeleteClient(cacheClient, output), nil
			}
			err = cacheClient.Set(courseId, newcs)
			if err != nil {
				log.Println(err)
				output["error"] = err.Error()
				return CacheClient.DeleteClient(cacheClient, output), nil
			}
			break
		}
	}
	output["success"] = true
	return CacheClient.DeleteClient(cacheClient, output), nil
}

type IsRegisterHandler struct {
	env             types.Environment
	connectionCache *CacheClient.ConnectionCache
}

func NewIsRegisterHandler(env types.Environment, connectionCache *CacheClient.ConnectionCache) types.FuncHandler {
	return &IsRegisterHandler{
		env:             env,
		connectionCache: connectionCache,
	}
}

func (h *IsRegisterHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	output := map[string]any{}
	cacheClient, params, err := CacheClient.NewClient(input, h.connectionCache)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}

	studentId := params["sid"].(string)
	courseId := params["cid"].(string)
	ss := &students{}
	err = json.Unmarshal(cacheClient.Get("students"), ss)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	exist := false
	for _, sid := range ss.StudentList {
		if sid == studentId {
			exist = true
			break
		}
	}
	if !exist {
		output["is"] = false
		return CacheClient.DeleteClient(cacheClient, output), nil
	}

	cs := &courses{}
	err = json.Unmarshal(cacheClient.Get("courses"), cs)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	exist = false
	for _, cid := range cs.CourseList {
		if cid == courseId {
			exist = true
			break
		}
	}
	if exist {
		es := &enrollments{}
		err = json.Unmarshal(cacheClient.Get(courseId), es)
		if err != nil {
			log.Println(err)
			output["error"] = err.Error()
			return CacheClient.DeleteClient(cacheClient, output), nil
		}
		if len(es.StudentList) == es.NumLimit {
			output["is"] = false
		} else {
			output["is"] = true
			output["sid"] = studentId
			output["cid"] = courseId
		}
	} else {
		output["is"] = false
	}
	return CacheClient.DeleteClient(cacheClient, output), nil
}

type IsDeleteHandler struct {
	env             types.Environment
	connectionCache *CacheClient.ConnectionCache
}

func NewIsDeleteHandler(env types.Environment, connectionCache *CacheClient.ConnectionCache) types.FuncHandler {
	return &IsDeleteHandler{
		env:             env,
		connectionCache: connectionCache,
	}
}

func (h *IsDeleteHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	output := map[string]any{}
	cacheClient, params, err := CacheClient.NewClient(input, h.connectionCache)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	courseId := params["cid"].(string)
	es := &enrollments{}
	err = json.Unmarshal(cacheClient.Get(courseId), es)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	if len(es.StudentList) == 0 {
		output["is"] = true
		output["cid"] = courseId
	} else {
		output["is"] = false
	}
	return CacheClient.DeleteClient(cacheClient, output), nil
}

type ListCoursesHandler struct {
	env             types.Environment
	connectionCache *CacheClient.ConnectionCache
}

func NewListCoursesHandler(env types.Environment, connectionCache *CacheClient.ConnectionCache) types.FuncHandler {
	return &ListCoursesHandler{
		env:             env,
		connectionCache: connectionCache,
	}
}

func (h *ListCoursesHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	output := map[string]any{}
	cacheClient, params, err := CacheClient.NewClient(input, h.connectionCache)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	studentId := params["sid"].(string)
	stu := &student{}
	err = json.Unmarshal(cacheClient.Get(studentId), stu)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	output["courses"] = stu.CourseList
	return CacheClient.DeleteClient(cacheClient, output), nil
}

type ResetCourseHandler struct {
	env             types.Environment
	connectionCache *CacheClient.ConnectionCache
}

func NewResetCourseHandler(env types.Environment, connectionCache *CacheClient.ConnectionCache) types.FuncHandler {
	return &ResetCourseHandler{
		env:             env,
		connectionCache: connectionCache,
	}
}

// 50 students and 50 courses with limit of 20 students every course
// student id: 0~49, course Id: 1000~1049
func (h *ResetCourseHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	output := map[string]any{}
	cacheClient, _, err := CacheClient.NewClient(input, h.connectionCache)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	log.Println("resetting")
	courselist := []string{}
	stulist := []string{}
	for i := 0; i < 50; i++ {
		courselist = append(courselist, strconv.Itoa(i+1000))
		stulist = append(stulist, strconv.Itoa(i))
	}

	ss := &students{StudentList: stulist}
	bss, _ := json.Marshal(ss)
	cs := &courses{CourseList: courselist}
	bcs, _ := json.Marshal(cs)

	es := &enrollments{NumLimit: 20, StudentList: []string{}}
	bes, _ := json.Marshal(es)
	stu := &student{CourseList: []string{}}
	bstu, _ := json.Marshal(stu)

	for _, cid := range cs.CourseList {
		err := cacheClient.Set(cid, bes)
		if err != nil {
			log.Println(err)
			output["error"] = err.Error()
			return CacheClient.DeleteClient(cacheClient, output), nil
		}
	}
	for _, sid := range ss.StudentList {
		err := cacheClient.Set(sid, bstu)
		if err != nil {
			log.Println(err)
			output["error"] = err.Error()
			return CacheClient.DeleteClient(cacheClient, output), nil
		}
	}

	err = cacheClient.Set("students", bss)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	err = cacheClient.Set("courses", bcs)
	if err != nil {
		log.Println(err)
		output["error"] = err.Error()
		return CacheClient.DeleteClient(cacheClient, output), nil
	}
	return CacheClient.DeleteClient(cacheClient, output), nil
}
