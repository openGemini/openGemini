// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package influxql

import (
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/openGemini/openGemini/lib/crypto"
)

const (
	ResourceType    = "type"
	LLMProviderType = "llm.provider_type"
	LLMEndpoint     = "llm.endpoint"
	LLMModelName    = "llm.model_name"
	LLMApiKey       = "llm.api_key"
	LLMTemperature  = "llm.temperature"
	LLMMaxTokens    = "llm.max_tokens"
	LLMMaxRetries   = "llm.max_retries"
	LLMRetryDelayMs = "llm.retry_delay_ms"

	DBKey   = "db"
	RPKey   = "rp"
	PropKey = "propKey"

	ExportDefaultSource    = "*"
	ExportDefaultSplitUnit = "1h"
	ExportDefaultDelay     = "10m"
	ExportDefaultTimeZone  = "Asia/Shanghai"

	SplitUnitMinDuration = 10 * time.Minute
	SplitUnitMaxDuration = 24 * time.Hour
	DelayMinDuration     = 5 * time.Minute
	DelayMaxDuration     = 20 * time.Minute
)

var apiKeyRegex = regexp.MustCompile(`^[a-zA-Z0-9_\-]+$`)

// GetJoinKeyByCondition used to extract the keys of the left and right expr of the join from the join condition.
func GetJoinKeyByCondition(joinCondition Expr, joinKeyMap map[string]string) error {
	if joinCondition == nil {
		return fmt.Errorf("join condition is null")
	}
	switch expr := joinCondition.(type) {
	case *ParenExpr:
		return GetJoinKeyByCondition(expr.Expr, joinKeyMap)
	case *BinaryExpr:
		if expr.Op != AND && expr.Op != EQ {
			return fmt.Errorf("only support the equal join")
		}
		lVal, ok1 := expr.LHS.(*VarRef)
		rVal, ok2 := expr.RHS.(*VarRef)
		if expr.Op == EQ && ok1 && ok2 {
			joinKeyMap[lVal.Val] = rVal.Val
		}
		if err := GetJoinKeyByCondition(expr.LHS, joinKeyMap); err != nil {
			return err
		}
		if err := GetJoinKeyByCondition(expr.RHS, joinKeyMap); err != nil {
			return err
		}
		return nil
	default:
		return nil
	}
}

// getJoinKeyByName used to extract the relevant join key according to the join table name or alias
func getJoinKeyByName(joinKeyMap map[string]string, mst string) []string {
	prefix := mst + "."
	joinKeys := make([]string, 0, len(joinKeyMap))
	for leftKey, rightKey := range joinKeyMap {
		if strings.HasPrefix(leftKey, prefix) {
			joinKeys = append(joinKeys, leftKey)
		} else if strings.HasPrefix(rightKey, prefix) {
			joinKeys = append(joinKeys, rightKey)
		}
	}
	sort.Strings(joinKeys)
	return joinKeys
}

// getAuxiliaryField used to get the auxiliary fields
func getAuxiliaryField(fields []*Field) []*Field {
	auxFields := make([]*Field, 0)
	for _, field := range fields {
		if field.Auxiliary {
			auxFields = append(auxFields, field)
		}
	}
	return auxFields
}

// rewriteAuxiliaryField used to rewrite the auxiliary fields to VarRef.
func rewriteAuxiliaryField(fields []*Field) []*Field {
	auxFields := make([]*Field, len(fields))
	for i := range fields {
		auxFields[i] = &Field{
			Expr:      &VarRef{Val: fields[i].Alias, Type: 0},
			Auxiliary: true,
			depth:     fields[i].depth,
		}
	}
	return auxFields
}

// rewriteAuxiliaryStmt used to rewrite the join query stmt with aux fields into a subquery,
// so that achieve the mapping of join fields that are not in select.
func rewriteAuxiliaryStmt(other *SelectStatement, sources []Source, auxFields []*Field,
	m FieldMapper, batchEn bool, hasJoin bool) (*SelectStatement, error) {
	oriFields := make([]*Field, 0, len(other.Fields))
	oriFields = append(oriFields, other.Fields...)
	other.Fields = append(other.Fields, rewriteAuxiliaryField(auxFields)...)
	var err error
	other, err = other.RewriteFields(m, batchEn, hasJoin)
	if err != nil {
		return nil, err
	}
	other.Sources = sources
	newQuery := other.Clone()
	other = &SelectStatement{
		Fields:     oriFields,
		Sources:    []Source{&SubQuery{Statement: newQuery, depth: 1 + newQuery.depth}},
		Dimensions: newQuery.Dimensions,
		StmtId:     newQuery.StmtId,
		OmitTime:   newQuery.OmitTime,
		TimeAlias:  newQuery.TimeAlias,
		depth:      2 + newQuery.depth,
	}
	return other, nil
}

// FieldSpec defines metadata for a single LLM resource property
type FieldSpec struct {
	Key         string   // Property key (e.g., "llm.provider_type")
	Required    bool     // Whether the field is mandatory
	Default     string   // Default value for optional fields
	ValidValues []string // Allowed values (empty = no restriction)
	Min         float64  // Minimum value for numeric fields (e.g., timeout_ms)
	Max         float64  // Maximum value for numeric fields
	Sensitive   bool     // Whether the field contains sensitive data (needs encryption)
	Description string   // Human-readable description of the field
}

// LLMFieldSpecs is the master specification for all supported LLM resource properties
var LLMFieldSpecs = map[string]FieldSpec{
	ResourceType: {
		Key:         ResourceType,
		Required:    true,
		ValidValues: []string{"llm"}, // Fixed to "llm" for LLM resources
		Description: "Resource type; must be 'llm' for LLM resources",
	},
	LLMProviderType: {
		Key:         LLMProviderType,
		Required:    true,
		ValidValues: []string{"openai", "gemini", "anthropic", "baidu", "huawei"}, // Supported LLM providers
		Description: "LLM service provider (e.g., openai, gemini)",
	},
	LLMEndpoint: {
		Key:         LLMEndpoint,
		Required:    true,
		Description: "API endpoint URL (e.g., https://api.openai.com/v1/chat/completions)",
	},
	LLMModelName: {
		Key:         LLMModelName,
		Required:    true,
		Description: "Name of the LLM model (e.g., gpt-4o, gemini-pro)",
	},
	LLMApiKey: {
		Key:         LLMApiKey,
		Required:    true,
		Sensitive:   true,
		Description: "Authentication key for API access",
	},
	LLMTemperature: {
		Key:         LLMTemperature,
		Required:    false,
		Default:     "0.7",
		Min:         0,
		Max:         1,
		Description: "Control the randomness of generated content with a floating-point number(0-1, default: 0.7)",
	},
	LLMMaxTokens: {
		Key:         LLMMaxTokens,
		Required:    false,
		Default:     "4096",
		Description: "Limit the maximum number of tokens generated, default: 4096",
	},
	LLMMaxRetries: {
		Key:         LLMMaxRetries,
		Required:    false,
		Default:     "3",
		Min:         1,
		Max:         10,
		Description: "Number of retry attempts on failure (1-10, default: 3)",
	},
	LLMRetryDelayMs: {
		Key:         LLMRetryDelayMs,
		Required:    false,
		Default:     "500",
		Min:         100,
		Max:         1000,
		Description: "The retry delay time milliseconds (100-1000, default: 500)",
	},
}

// ValidateAndFillProperties checks properties against specs and fills default values
// Returns validated properties or an error if validation fails
func ValidateAndFillProperties(props map[string]string) (map[string]string, error) {
	// 1. Check for required fields
	for _, spec := range LLMFieldSpecs {
		if spec.Required {
			if specValue, exists := props[spec.Key]; !exists || len(specValue) == 0 {
				return nil, fmt.Errorf("missing required field: %s (desc: %s)", spec.Key, spec.Description)
			}
		}
	}

	// 2. Fill default values for optional fields not explicitly provided
	filledProps := make(map[string]string)
	for k, v := range props {
		filledProps[k] = v
	}
	for _, spec := range LLMFieldSpecs {
		if !spec.Required { // Only process optional fields
			if _, exists := filledProps[spec.Key]; !exists {
				filledProps[spec.Key] = spec.Default
			}
		}
	}

	// 3. Validate each field against its specification
	for key, value := range filledProps {
		spec, exists := LLMFieldSpecs[key]
		if !exists {
			return nil, fmt.Errorf("unsupported field: %s", key)
		}

		// Validate against allowed values (e.g., "type" must be "llm")
		if len(spec.ValidValues) > 0 {
			isValid := false
			for _, allowed := range spec.ValidValues {
				if value == allowed {
					isValid = true
					break
				}
			}
			if !isValid {
				return nil, fmt.Errorf("invalid value for %s; allowed: %v", key, spec.ValidValues)
			}
		}

		// Validate URL format for endpoint
		if key == LLMEndpoint {
			if value != "" { // Skip empty proxies
				if _, err := url.ParseRequestURI(value); err != nil {
					return nil, fmt.Errorf("invalid URL for %s: %v", key, err)
				}
			}
		}

		// Validate numeric fields (timeout_ms/max_retries)
		if key == LLMTemperature || key == LLMMaxRetries || key == LLMRetryDelayMs {
			num, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, fmt.Errorf("%s must be float: %v", key, err)
			}
			if num < spec.Min || num > spec.Max {
				return nil, fmt.Errorf("%s must be between %v and %v", key, spec.Min, spec.Max)
			}
		}

		// Validate api_key format (if provided)
		if key == LLMApiKey && value != "" {
			// Reject invalid characters (common API key constraints)
			if !apiKeyRegex.MatchString(value) {
				return nil, fmt.Errorf("%s contains invalid characters (allow: letters, numbers, _, -)", key)
			}
		}
	}
	return filledProps, nil
}

// EncryptSensitiveFields encrypts sensitive properties per LLMFieldSpecs
// Returns encrypted properties or error on encryption failure
func EncryptSensitiveFields(props map[string]string) (map[string]string, error) {
	encrypted := make(map[string]string)
	for k, v := range props {
		spec, exists := LLMFieldSpecs[k]
		if !exists {
			continue // Should be caught by validation earlier
		}

		// Encrypt only sensitive, non-empty fields
		if spec.Sensitive && v != "" {
			encVal, err := crypto.Encrypt(v)
			if err != nil {
				return nil, fmt.Errorf("failed to encrypt %s: %v", k, err)
			}
			encrypted[k] = encVal
		} else {
			encrypted[k] = v // Store non-sensitive fields as-is
		}
	}
	return encrypted, nil
}

// DecryptSensitiveFields decrypts sensitive fields if the user has permission
// Returns display-ready properties (masked if no permission)
func DecryptSensitiveFields(props map[string]string, hasPermission bool) (map[string]string, error) {
	displayProps := make(map[string]string)
	for k, v := range props {
		spec, exists := LLMFieldSpecs[k]
		if !exists {
			displayProps[k] = v
			continue
		}

		if spec.Sensitive {
			if hasPermission && v != "" { // Decrypt only if authorized
				plainVal := crypto.Decrypt(v)
				displayProps[k] = plainVal
			} else {
				displayProps[k] = "******" // Mask sensitive data for unauthorized users
			}
		} else {
			displayProps[k] = v // Non-sensitive fields shown as-is
		}
	}
	return displayProps, nil
}

// TaskFactory is a map that associates task types with functions that create tasks.
var TaskFactory = map[TaskType]func() Task{
	TaskExport: func() Task { return new(ExportTask) },
}

// globalValidator is a global validator instance that registers custom validation functions.
var globalValidator = func() *validator.Validate {
	v := validator.New()
	_ = v.RegisterValidation("split_unit", NewTimeRangeValidator(SplitUnitMinDuration, SplitUnitMaxDuration))
	_ = v.RegisterValidation("delay", NewTimeRangeValidator(DelayMinDuration, DelayMaxDuration))
	_ = v.RegisterValidation("timezone", validateTimeZone)
	return v
}()

func NewTimeRangeValidator(min, max time.Duration) validator.Func {
	return func(fl validator.FieldLevel) bool {
		timeStr := fl.Field().String()

		duration, err := time.ParseDuration(timeStr)
		if err != nil {
			return false
		}

		return duration >= min && duration <= max
	}
}

// validateTimeZone checks if a time zone is valid.
func validateTimeZone(fl validator.FieldLevel) bool {
	timeZone := fl.Field().String()
	_, err := time.LoadLocation(timeZone)
	return err == nil
}

// Task is an interface that defines methods for filling properties, converting to a map, and getting default configuration
type Task interface {
	Fill(prop map[string]string) error
	ToMap() (map[string]string, error)
	DefaultConfig() Task
}

// ExportTask represents a specific type of task for exporting data
type ExportTask struct {
	Db        string `validate:"required" propKey:"db"`
	Rp        string `validate:"required" propKey:"rp"`
	Format    string `validate:"required,eq=parquet" propKey:"format"`
	Source    string `validate:"required" propKey:"source"`
	SplitUnit string `validate:"required,split_unit" propKey:"split_unit"`
	Delay     string `validate:"required,delay" propKey:"delay"`
	TimeZone  string `validate:"timezone" propKey:"time_zone"`
	Target    string `validate:"omitempty" propKey:"target"`
}

func (t *ExportTask) Fill(in map[string]string) error {
	return Fill(in, t)
}

func (t *ExportTask) ToMap() (map[string]string, error) {
	return ToMap(t)
}

// DefaultConfig returns the default configuration for an ExportTask.
func (t *ExportTask) DefaultConfig() Task {
	return &ExportTask{
		Source:    ExportDefaultSource,
		SplitUnit: ExportDefaultSplitUnit,
		Delay:     ExportDefaultDelay,
		TimeZone:  ExportDefaultTimeZone,
	}
}

// ValidateAndFillPropertiesByType validates and fills properties of a task based on its type.
func ValidateAndFillPropertiesByType(taskType TaskType, propIn map[string]string) (map[string]string, error) {
	taskCreator, ok := TaskFactory[taskType]
	if !ok {
		return nil, fmt.Errorf("unknown task type %q", taskType)
	}
	defaultTask := taskCreator().DefaultConfig()
	if err := defaultTask.Fill(propIn); err != nil {
		return nil, err
	}
	if err := globalValidator.Struct(defaultTask); err != nil {
		return nil, err
	}
	return defaultTask.ToMap()
}

func Fill(m map[string]string, s interface{}) error {
	val := reflect.ValueOf(s)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return errors.New("s must be a non-nil struct pointer")
	}
	structVal := val.Elem()
	if structVal.Kind() != reflect.Struct {
		return errors.New("s must point to a struct")
	}

	structType := structVal.Type()
	for i := 0; i < structType.NumField(); i++ {
		fieldType := structType.Field(i)
		fieldVal := structVal.Field(i)

		if fieldVal.Kind() != reflect.String || !fieldVal.CanSet() {
			continue
		}

		mapKey := fieldType.Tag.Get(PropKey)
		if mapKey == "" {
			mapKey = fieldType.Name
		}

		if v, ok := m[mapKey]; ok {
			fieldVal.SetString(v)
		}
	}
	return nil
}

func ToMap(s interface{}) (map[string]string, error) {
	val := reflect.ValueOf(s)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, errors.New("struct pointer is nil")
		}
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil, errors.New("input must be a struct or struct pointer")
	}

	m := make(map[string]string)
	structType := val.Type()
	for i := 0; i < structType.NumField(); i++ {
		fieldType := structType.Field(i)
		fieldVal := val.Field(i)

		if fieldVal.Kind() != reflect.String || !fieldVal.CanInterface() {
			continue
		}

		mapKey := fieldType.Tag.Get(PropKey)
		if mapKey == "" {
			mapKey = fieldType.Name
		}
		m[mapKey] = fieldVal.String()
	}
	return m, nil
}

func TranSetIntoOrExpr(cond Expr) Expr {
	if cond == nil {
		return cond
	}
	switch expr := cond.(type) {
	case *ParenExpr:
		return TranSetIntoOrExpr(expr.Expr)
	case *BinaryExpr:
		expr.LHS = TranSetIntoOrExpr(expr.LHS)
		expr.RHS = TranSetIntoOrExpr(expr.RHS)
		if expr.Op == IN {
			expr = TranInIntoOrExpr(expr)
			cond = &ParenExpr{Expr: expr, depth: expr.Depth()}
			return cond
		}
	default:
	}
	return cond
}

func TranInIntoOrExpr(expr *BinaryExpr) *BinaryExpr {
	if expr == nil || expr.Op != IN {
		return expr
	}
	lhs, ok := expr.LHS.(*VarRef)
	if !ok {
		return expr
	}
	rhs, ok := expr.RHS.(*SetLiteral)
	if !ok || len(rhs.Vals) == 0 {
		return expr
	}
	var res *BinaryExpr
	for k := range rhs.Vals {
		lhsBinary := &BinaryExpr{Op: EQ, LHS: lhs, depth: 1}
		switch lhs.Type {
		case Integer:
			lhsBinary.RHS = &IntegerLiteral{Val: k.(int64)}
		case Float:
			lhsBinary.RHS = &NumberLiteral{Val: k.(float64)}
		case Boolean:
			lhsBinary.RHS = &BooleanLiteral{Val: k.(bool)}
		case String, Tag:
			lhsBinary.RHS = &StringLiteral{Val: k.(string)}
		default:
		}
		if res == nil {
			res = lhsBinary
			continue
		}
		res = &BinaryExpr{Op: OR, LHS: lhsBinary, RHS: res, depth: lhsBinary.Depth() + res.Depth()}
	}
	return res
}
