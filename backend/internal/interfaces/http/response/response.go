package response

func Success(data interface{}) response {
	return response{
		Code:    200,
		Message: "success",
		Data:    data,
	}
}

func Error(code int, msg string) response {
	return response{
		Code:    code,
		Message: msg,
	}
}

func ErrorWithData(code int, msg string, data interface{}) response {
	return response{
		Code:    code,
		Message: msg,
		Data:    data,
	}
}
