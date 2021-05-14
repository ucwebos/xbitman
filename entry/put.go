package entry

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"xbitman/ecode"
	"xbitman/index"
)

// PutReq .
type PutReq struct {
	Data []map[string]interface{} `json:"data"` //data
}

// PutResp .
type PutResp struct {
	Success bool `json:"success"`
}

func put(ctx *gin.Context) {
	var (
		table = ctx.Param("table")
		req   = PutReq{}
		resp  = PutResp{}
	)
	err := BindParams(ctx, &req)
	if err != nil {
		JSONError(ctx, ecode.ErrUser, "")
		return
	}
	tb := index.DB.Table(table)
	if tb == nil {
		JSONError(ctx, ecode.ErrUser, fmt.Sprintf("not found table[%s]", table))
		return
	}
	if len(req.Data) > 1000 {
		JSONError(ctx, ecode.ErrUser, fmt.Sprintf("A maximum of 1000 items are allowed [%s]", table))
		return
	}
	err = tb.PutBatch(req.Data)
	if err != nil {
		JSONError(ctx, ecode.ErrUser, fmt.Sprintf("PutBatch err [%v]", err))
		return
	}
	resp.Success = true
	JSONSuccess(ctx, resp)
}
