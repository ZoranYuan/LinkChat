package ws

type ErrorNoticeEvent struct {
	RequestOp string `json:"requestOp"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}
