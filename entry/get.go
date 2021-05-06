package entry

import (
	"encoding/json"
	"fmt"
	"github.com/cstockton/go-conv"
	"github.com/gin-gonic/gin"
	"xbitman/ecode"
	"xbitman/index"
)

// GetReq .
type GetReq struct {
	IDs []interface{} `json:"ids"`
}

// GetResp .
type GetResp struct {
	List []json.RawMessage `json:"list"`
}

func get(ctx *gin.Context) {
	var (
		table = ctx.Param("table")
		req   = GetReq{}
		resp  = GetResp{}
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
	ids := make([]string, len(req.IDs))
	for i, d := range req.IDs {
		ids[i], _ = conv.String(d)
	}
	list := make([]json.RawMessage, 0)
	if len(ids) == 0 {
		raw, err := tb.Get(ids[0])
		if err != nil {
			JSONError(ctx, ecode.ErrSystem, fmt.Sprintf("query err [%v]", err))
			return
		}
		list = append(list, raw)
	}
	list, err = tb.Gets(ids)
	if err != nil {
		JSONError(ctx, ecode.ErrSystem, fmt.Sprintf("query err [%v]", err))
		return
	}
	resp.List = list
	JSONSuccess(ctx, resp)
}
