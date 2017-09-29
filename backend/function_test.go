// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"log"
	"testing"
)

func BenchmarkGetmax(b *testing.B) {
	tests := []struct {
		sets []interface{}
	}{
		{
			sets: []interface{}{float32(-4), float32(-2), float32(-1)},
		},
		{
			sets: []interface{}{float32(-4), float32(-2), float32(0)},
		},
		{
			sets: []interface{}{float32(0), float32(1), float32(2)},
		},
		{
			sets: []interface{}{float32(1), float32(2), float32(4)},
		},
		{
			sets: []interface{}{float32(-2), float32(0), float32(2)},
		},
	}
	for _, v := range tests {
		_ = getmax(v.sets)
	}
}

func BenchmarkGetmin(b *testing.B) {
	tests := []struct {
		sets []interface{}
	}{
		{
			sets: []interface{}{float32(-4), float32(-2), float32(-1)},
		},
		{
			sets: []interface{}{float32(-4), float32(-2), float32(0)},
		},
		{
			sets: []interface{}{float32(0), float32(1), float32(2)},
		},
		{
			sets: []interface{}{float32(1), float32(2), float32(4)},
		},
		{
			sets: []interface{}{float32(-2), float32(0), float32(2)},
		},
	}
	for _, v := range tests {
		_ = getmin(v.sets)
	}
}

func BenchmarkGetsum(b *testing.B) {
	tests := []struct {
		sets []interface{}
	}{
		{
			sets: []interface{}{float32(-4), float32(-2), float32(-1)},
		},
		{
			sets: []interface{}{float32(-4), float32(-2), float32(0)},
		},
		{
			sets: []interface{}{float32(0), float32(1), float32(2)},
		},
		{
			sets: []interface{}{float32(1), float32(2), float32(4)},
		},
		{
			sets: []interface{}{float32(-2), float32(0), float32(2)},
		},
	}
	for _, v := range tests {
		_ = getsum(v.sets)
	}
}

func BenchmarkGetmean(b *testing.B) {
	tests := []struct {
		sets []interface{}
	}{
		{
			sets: []interface{}{float32(-4), float32(-2), float32(-1)},
		},
		{
			sets: []interface{}{float32(-4), float32(-2), float32(0)},
		},
		{
			sets: []interface{}{float32(0), float32(1), float32(2)},
		},
		{
			sets: []interface{}{float32(1), float32(2), float32(4)},
		},
		{
			sets: []interface{}{float32(-2), float32(0), float32(2)},
		},
	}
	for _, v := range tests {
		log.Println(getmean(v.sets, v.sets))
	}
}
