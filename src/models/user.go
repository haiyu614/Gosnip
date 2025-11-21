package models

import (
	"Gosnip/src/storage/database"
	"regexp"

	"gorm.io/gorm"
)

type User struct {
	ID              uint32 `gorm:"not null;primarykey;autoIncrement"`               //用户 Id
	UserName        string `gorm:"not null;unique;size: 32;index" redis:"UserName"` // 用户名
	Password        string `gorm:"not null" redis:"Password"`                       // 密码
	Role            int    `gorm:"default:1" redis:"Role"`                          // 角色
	Avatar          string `redis:"Avatar"`                                         // 头像
	BackgroundImage string `redis:"BackGroundImage"`                                // 背景图片
	Signature       string `redis:"Signature"`                                      // 个人简介
	gorm.Model
}

// IsNameEmail 判断用户的名称是否为邮箱。
func (u *User) IsNameEmail() bool {
	pattern := `\w+([-+.]\w+)*@\w+([-.]\w+)*\.\w+([-.]\w+)*`
	reg := regexp.MustCompile(pattern)
	return reg.MatchString(u.UserName)
}

func (u *User) IsDirty() bool {
	return u.UserName != "" // todo 不应该是用户名为空时返回 true 吗
}

func (u *User) GetID() uint32 {
	return u.ID
}

func init() {
	if err := database.Client.AutoMigrate(&User{}); err != nil {
		panic(err)
	}
}
