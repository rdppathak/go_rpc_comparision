package utils

import (
	"time"
)

func Decorator(f func(s string)) func(string) {
	return func(s string) {
		fmt.Println("before")
		f(s)
		fmt.Println("after")
	}
}