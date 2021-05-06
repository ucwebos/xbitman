package entry

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"xbitman/conf"
	"xbitman/ecode"
	"xbitman/index"
)

func tables(ctx *gin.Context) {
	tables := index.DB.Tables()
	JSONSuccess(ctx, tables)
}

func tableCreate(ctx *gin.Context) {
	var (
		req = conf.Table{}
	)
	err := BindParams(ctx, &req)
	if err != nil {
		JSONError(ctx, ecode.ErrUser, "")
		return
	}
	if req.Name == "" || req.PKey == nil || req.Indexes == nil {
		JSONError(ctx, ecode.ErrUser, "表结构错误")
		return
	}
	err = index.DB.CreateTable(req.Name, &req)
	if err != nil {
		JSONError(ctx, ecode.ErrSystem, fmt.Sprintf("table create err [%v]", err))
		return
	}
	JSONSuccess(ctx, map[string]interface{}{"ok": true})
}

func tableUpdate(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, "")
}

func tableRename(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, "")
}

func tableDelete(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, "")
}
