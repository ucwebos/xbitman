package ecode

type Err struct {
	Code int
	Msg  string
}

var (
	SUCCESS       = New(0, "OK")
	ErrParams     = New(-1, "请求参数错误")
	ErrParamsSign = New(-2, "签名错误")
	ErrSystem     = New(-3, "内部错误,请稍后重试")
	ErrUser       = New(-4, "用户操作错误")
	ErrNeedLogin  = New(-5, "用户未登录或者登录失效")
	ErrUserName   = New(-6, "用户昵称已被占用")
)

func New(code int, msg string) *Err {
	return &Err{
		Code: code,
		Msg:  msg,
	}
}
