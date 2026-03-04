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
// 非空接口的底层结构
type ITab struct {
	Inter *InterfaceType
	Type  *Type
	Hash  uint32 // copy of Type.Hash. Used for type switches.
	//接口的方法表数组，这个可以在动态派发的时候快速找到方法地址，而不用去Type中去找了，
	//每个元素是指向方法地址的指针，方法数组大小实际是动态的，不只是1，todo会根据Inter中的方法数?，在Fun[0]后面加上其余的的方法
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
