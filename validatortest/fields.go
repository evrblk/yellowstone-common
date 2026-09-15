// Package validatortest statically checks that a hand-written proto request
// validator references every field declared on the proto message it
// validates, so that a newly added proto field cannot silently slip past
// validation.
package validatortest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"runtime"
	"sort"
	"strings"
)

// TestingT is the subset of testing.TB that AssertAllFieldsChecked needs.
// *testing.T and *testing.B both satisfy it.
type TestingT interface {
	Helper()
	Fatalf(format string, args ...any)
}

// AssertAllFieldsChecked asserts, via static analysis of validateFunc's own
// source, that every proto field declared on validateFunc's first parameter
// is referenced somewhere in its body, except for fields named in
// skipFields (for fields that genuinely need no validation).
//
// validateFunc is typically a hand-written request validator following the
// convention used throughout this codebase:
//
//	func ValidateCreateQueueRequest(req *moabpb.CreateQueueRequest) error
//
// but any function whose first parameter is a pointer to a struct works,
// including narrower per-field validators with extra trailing parameters:
//
//	func validateRetryStrategy(value *moabpb.RetryStrategy, fieldName string) error
//
// Only fields carrying a `protobuf:` or `protobuf_oneof:` struct tag are
// required to be referenced — this is exactly the set of fields
// protoc-gen-go generates for a message, so struct-internal bookkeeping
// fields (state, sizeCache, unknownFields) are ignored automatically. This
// makes the check generic across any protoc-gen-go-generated message, in
// this repo or elsewhere.
//
// The check is purely syntactic: it looks for direct selector expressions
// of the form `<param>.<Field>` anywhere in the function's source
// (including inside nested calls, loops, and switches), and does not
// evaluate whether the referencing code actually validates the field
// correctly, or whether nested message fields are themselves checked by
// whatever the field is passed to. It also cannot see through field
// accesses made via an intermediate alias variable — write validators that
// read the field straight off the parameter.
func AssertAllFieldsChecked(t TestingT, validateFunc any, skipFields ...string) {
	t.Helper()

	fnVal := reflect.ValueOf(validateFunc)
	if fnVal.Kind() != reflect.Func {
		t.Fatalf("validatortest: validateFunc must be a function, got %s", fnVal.Kind())
		return
	}

	fnType := fnVal.Type()
	if fnType.NumIn() < 1 {
		t.Fatalf("validatortest: validateFunc must take at least one argument")
		return
	}

	paramType := fnType.In(0)
	if paramType.Kind() != reflect.Pointer || paramType.Elem().Kind() != reflect.Struct {
		t.Fatalf("validatortest: validateFunc's first argument must be a pointer to a struct, got %s", paramType)
		return
	}

	rtFunc := runtime.FuncForPC(fnVal.Pointer())
	if rtFunc == nil {
		t.Fatalf("validatortest: could not resolve validateFunc to a named function")
		return
	}
	shortName := funcShortName(rtFunc.Name())

	file, _ := rtFunc.FileLine(rtFunc.Entry())
	fset := token.NewFileSet()
	astFile, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("validatortest: failed to parse source of %s (%s): %v", shortName, file, err)
		return
	}

	decl := findFuncDecl(astFile, shortName)
	if decl == nil || decl.Body == nil {
		t.Fatalf("validatortest: could not find source of function %s in %s", shortName, file)
		return
	}
	if decl.Type.Params == nil || len(decl.Type.Params.List) == 0 || len(decl.Type.Params.List[0].Names) == 0 {
		t.Fatalf("validatortest: function %s has no named parameters", shortName)
		return
	}
	paramName := decl.Type.Params.List[0].Names[0].Name

	referenced := referencedSelectors(decl.Body, paramName)

	skip := make(map[string]struct{}, len(skipFields))
	for _, f := range skipFields {
		skip[f] = struct{}{}
	}

	var missing []string
	for _, f := range protoFieldNames(paramType.Elem()) {
		if _, ok := skip[f]; ok {
			continue
		}
		if _, ok := referenced[f]; !ok {
			missing = append(missing, f)
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("%s does not reference proto field(s): %s", shortName, strings.Join(missing, ", "))
	}
}

// protoFieldNames returns the Go field names of structType that carry a
// `protobuf:` or `protobuf_oneof:` tag, i.e. the fields protoc-gen-go
// generated for the corresponding proto message.
func protoFieldNames(structType reflect.Type) []string {
	var names []string
	for f := range structType.Fields() {
		f := f
		if !f.IsExported() {
			continue
		}
		_, hasTag := f.Tag.Lookup("protobuf")
		_, hasOneofTag := f.Tag.Lookup("protobuf_oneof")
		if !hasTag && !hasOneofTag {
			continue
		}
		names = append(names, f.Name)
	}
	return names
}

// referencedSelectors returns the set of field names selected off of an
// identifier named paramName anywhere within body (e.g. `req.QueueName`
// contributes "QueueName").
func referencedSelectors(body *ast.BlockStmt, paramName string) map[string]struct{} {
	referenced := map[string]struct{}{}
	ast.Inspect(body, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == paramName {
			referenced[sel.Sel.Name] = struct{}{}
		}
		return true
	})
	return referenced
}

// funcShortName extracts the bare function name from a runtime.Func's fully
// qualified name (e.g. "github.com/evrblk/moab/pkg/server/v0.ValidateCreateQueueRequest"
// -> "ValidateCreateQueueRequest").
func funcShortName(fullName string) string {
	tail := fullName
	if idx := strings.LastIndex(fullName, "/"); idx >= 0 {
		tail = fullName[idx+1:]
	}
	if idx := strings.LastIndex(tail, "."); idx >= 0 {
		return tail[idx+1:]
	}
	return tail
}

// findFuncDecl finds the top-level, non-method function declaration named
// name in file.
func findFuncDecl(file *ast.File, name string) *ast.FuncDecl {
	for _, d := range file.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Name.Name == name {
			return fd
		}
	}
	return nil
}
