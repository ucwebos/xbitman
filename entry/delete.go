package entry

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"xbitman/ecode"
	"xbitman/index"
)

// DelReq .
type DelReq struct {
	Table []string `json:"ids"`
}

// DelResp .
type DelResp struct {
	Success bool `json:"success"`
}

func del(ctx *gin.Context) {
	var (
		table = ctx.Param("table")
	)

	tb := index.DB.Table(table)
	if tb == nil {
		JSONError(ctx, ecode.ErrUser, fmt.Sprintf("not found table[%s]", table))
		return
	}

	// todo ...

}
