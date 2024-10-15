// Code generated by MockGen. DO NOT EDIT.
// Source: ./indexrepo.go
//
// Generated by this command:
//
//	mockgen -source=./indexrepo.go -destination=indexrepo_mock_test.go -package=indexrepo_test
//

// Package indexrepo_test is a generated GoMock package.
package indexrepo_test

import (
	context "context"
	reflect "reflect"

	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	gomock "go.uber.org/mock/gomock"
)

// MockObjectGetter is a mock of ObjectGetter interface.
type MockObjectGetter struct {
	ctrl     *gomock.Controller
	recorder *MockObjectGetterMockRecorder
}

// MockObjectGetterMockRecorder is the mock recorder for MockObjectGetter.
type MockObjectGetterMockRecorder struct {
	mock *MockObjectGetter
}

// NewMockObjectGetter creates a new mock instance.
func NewMockObjectGetter(ctrl *gomock.Controller) *MockObjectGetter {
	mock := &MockObjectGetter{ctrl: ctrl}
	mock.recorder = &MockObjectGetterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockObjectGetter) EXPECT() *MockObjectGetterMockRecorder {
	return m.recorder
}

// GetObject mocks base method.
func (m *MockObjectGetter) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, params}
	for _, a := range optFns {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetObject", varargs...)
	ret0, _ := ret[0].(*s3.GetObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetObject indicates an expected call of GetObject.
func (mr *MockObjectGetterMockRecorder) GetObject(ctx, params any, optFns ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, params}, optFns...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetObject", reflect.TypeOf((*MockObjectGetter)(nil).GetObject), varargs...)
}

// PutObject mocks base method.
func (m *MockObjectGetter) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.ctrl.T.Helper()
	varargs := []any{ctx, params}
	for _, a := range optFns {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PutObject", varargs...)
	ret0, _ := ret[0].(*s3.PutObjectOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PutObject indicates an expected call of PutObject.
func (mr *MockObjectGetterMockRecorder) PutObject(ctx, params any, optFns ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx, params}, optFns...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutObject", reflect.TypeOf((*MockObjectGetter)(nil).PutObject), varargs...)
}
