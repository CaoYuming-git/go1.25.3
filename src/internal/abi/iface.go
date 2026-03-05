// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package abi

import "unsafe"

// ITab The first word of every non-empty interface type contains an *ITab.
// It records the underlying concrete type (Type), the interface type it
// is implementing (Inter), and some ancillary information.
//
// allocated in non-garbage-collected memory
// 非空接口的具体类型实现的接口方法表
type ITab struct {
	// 接口类型的类型元数据，描述接口本身的
	Inter *InterfaceType
	// 接口指向的实现类型的类型的类型元数据
	Type *Type
	// Type的哈希值
	Hash uint32 // copy of Type.Hash. Used for type switches.
	// 具体类型实现的接口的方法的方法地址表，这个可以在动态派发的时候快速找到调用的方法地址，而不用去Type中去找了
	// 这个数组的大小实际是动态的，会在Fun紧跟着所有实现的方法集地址
	// Fun[0]如果为0，表示具体类型没有实现接口
	Fun [1]uintptr // variable sized. fun[0]==0 means Type does not implement Inter.
}

// EmptyInterface describes the layout of a "interface{}" or a "any."
// These are represented differently than non-empty interface, as the first
// word always points to an abi.Type.
type EmptyInterface struct {
	Type *Type
	Data unsafe.Pointer
}

// NonEmptyInterface describes the layout of an interface that contains any methods.
type NonEmptyInterface struct {
	ITab *ITab
	Data unsafe.Pointer
}
