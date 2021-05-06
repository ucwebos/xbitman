package entry

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"xbitman/ecode"
	"xbitman/index"
)

type QueryReq struct {
	Query index.Op     `json:"query"`
	Limit *index.Limit `json:"limit,omitempty"`
	Sort  *index.Sort  `json:"sort,omitempty"`
}

type QueryResp struct {
	List  []json.RawMessage
	Total int `json:"total"`
}

func query(ctx *gin.Context) {
	var (
		table = ctx.Param("table")
		qs    = QueryReq{}
		resp  = QueryResp{}
	)
	err := BindParams(ctx, &qs)
	if err != nil {
		JSONError(ctx, ecode.ErrUser, "")
		return
	}
	tb := index.DB.Table(table)
	if tb == nil {
		JSONError(ctx, ecode.ErrUser, fmt.Sprintf("not found table[%s]", table))
		return
	}
	list, total, err := tb.Query(qs.Query, qs.Limit, qs.Sort)
	if err != nil {
		JSONError(ctx, ecode.ErrSystem, fmt.Sprintf("query err [%v]", err))
		return
	}
	resp.List = list
	resp.Total = total
	JSONSuccess(ctx, resp)
}

func count(ctx *gin.Context) {
	var (
		table = ctx.Param("table")
		qs    = QueryReq{}
	)
	err := BindParams(ctx, &qs)
	if err != nil {
		JSONError(ctx, ecode.ErrUser, "")
		return
	}
	tb := index.DB.Table(table)
	if tb == nil {
		JSONError(ctx, ecode.ErrUser, fmt.Sprintf("not found table[%s]", table))
		return
	}
	total, err := tb.Count(qs.Query)
	if err != nil {
		JSONError(ctx, ecode.ErrSystem, fmt.Sprintf("query err [%v]", err))
		return
	}
	JSONSuccess(ctx, total)
}

func scan() {
	// Cursor id
}
