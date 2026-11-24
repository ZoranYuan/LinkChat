package agent

import (
	"errors"
)

var ErrGroupAgentAccessDenied = errors.New("用户无权访问该群聊")
