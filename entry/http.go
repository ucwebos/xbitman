package entry

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/render"
	"net/http"
	"time"
	"xbitman/ecode"
	"xbitman/libs"
)

// Run .
func Run() {
	r := gin.Default()
	gin.SetMode(gin.ReleaseMode)
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "pong",
		})
	})

	r.POST("/:table/query", query)
	r.POST("/:table/count", count)
	r.GET("/:table/get", get)
	r.POST("/:table/put", put)
	r.DELETE("/:table/delete", del)

	r.GET("/tables", tables)
	r.POST("/table/create", tableCreate)
	r.POST("/table/update", tableUpdate)
	r.POST("/table/rename", tableRename)
	r.DELETE("/table/delete", tableDelete)

	r.Run(":7878")
}

// BindParams 获取参数
func BindParams(ctx *gin.Context, dst interface{}) (err error) {
	r, err := ctx.GetRawData()
	if err != nil {
		JSONError(ctx, ecode.ErrParams, "")
	}
	return libs.JSON.Unmarshal(r, &dst)
}

// Resp http 返回的结构体
type Resp struct {
	Code int         `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
	Ts   int64       `json:"ts"`
}

// JSONSuccess 成功返回
func JSONSuccess(ctx *gin.Context, data interface{}) {
	var (
		code int
		msg  string
	)
	res := Resp{
		Code: code,
		Msg:  msg,
		Data: data,
		Ts:   time.Now().Unix(),
	}
	JSONResp(ctx, res)
}

// JSONError 失败返回
func JSONError(ctx *gin.Context, err *ecode.Err, msg string) {
	if msg == "" {
		msg = err.Msg
	}
	res := Resp{
		Code: err.Code,
		Msg:  msg,
		Data: struct{}{},
		Ts:   time.Now().Unix(),
	}
	JSONResp(ctx, res)
}

// JSONResp .
func JSONResp(ctx *gin.Context, res interface{}) {
	ctx.Render(http.StatusOK, render.JSON{Data: res})
	ctx.Abort()
	return
}
