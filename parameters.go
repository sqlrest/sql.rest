// TODO This file needs a lot of TLC. Mainly, too much duplication.
package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"text/template"

	_ "github.com/lib/pq"
)

//
var formatsParameters = map[string]reflect.Type{
	"_float": reflect.TypeOf(float64(1.0)),
	"_int":   reflect.TypeOf(int64(1)),
}

//
var formats = map[reflect.Type]string{
	formatsParameters["_float"]: "g",
	formatsParameters["_int"]:   "d",
}

//
var re_staticVariable = regexp.MustCompile(`[{]{2}[.]([^}]+)[}]{2}`)
var re_missingVariable = regexp.MustCompile(`/-/([^/]+)/-/`)

// Cleanse the parameters. TODO this will be added to the model's validation rules.
func cleanse(n, v string) (s string) {
	if l := len(n); l > 30 {
		log.Printf("WARNING: skipping very long query parameter name: %s... %d", n[:30], l)
		return
	}
	if len(v) == 0 {
		log.Printf("WARNING: skipping empty value for %s", n)
		return
	}
	if strings.HasPrefix(n, ".") {
		log.Printf("WARNING: skipping internal parameter name: %s", n)
		return
	}
	if strings.HasPrefix(n, "_") {
		log.Printf("WARNING: skipping parameter name: %s", n)
		return
	}
	// Truncate the values. Nothing should be longer than 100 characters.
	if l := len(v); l > 50 {
		log.Printf("WARNING: skipping very long query parameter: %s... %d", v[:50], l)
		return
	}
	// TODO does this need to check for percent encoded or is it already decoded by the time it reaches here.
	s = v
	s = strings.Replace(s, `;`, ``, -1)
	s = strings.Replace(s, `'`, ``, -1)
	s = strings.Replace(s, `"`, ``, -1)
	return
}

// This function produces a query and bind-variable slice from a template-string and two variable-maps.
//
// First, there are two types of variables used in the model:
//
//   {{name}}  - these are converted into bind-variables and can come from the user or the model.
//   {{.name}} - these are replaced verbatim in the template.
//
// Though those are defined in the model, the name provided in the url, query, model, are always just "name".
//
// The {{name}}-type variables are converted into binds as follows:
//
// For example, for the query:
//
//   select * from ({{.source}}) as source where name={{name}}::text and value={{value}}::bigint
//
// and given variables:
//   name="abc"
//   value=123
//   source="select 'abc'::text as name, 123 as value"
//
// the above query will be converted into:
//
//   select * from (select 'abc'::text as name, 123 as value) as source where name=$1::text and value=$2::bigint
//
// with the bind-variables pass with the query:
//
//   ["abc", 123]
//
//
func parameterize(w http.ResponseWriter, req *http.Request, sqlText string) (string, []interface{}) {

	// log.Printf("DEBUG: parameters:\n\tsqlText=%s\n\n", sqlText)

	// Combine the query parameters, url parameters into a single map.
	vars := map[string]string{}

	// Cleanse and add query parameters to the variables map.
	for n, vs := range req.URL.Query() {
		// Query parameter values are arrays. Only pull the first value.
		// log.Printf("DEBUG: query parameter: %s=%s", n, vs)
		if len(vs) >= 1 { // TODO support arrays, e.g. for use with SQL in-clauses.
			v := vs[0]
			if s := cleanse(n, v); s != "" {
				vars[n] = s
				// log.Printf("DEBUG: query parameter: %s=%s", n, s)
			}
		}
	}

	// Cleanse and add query parameters to the variables map.
	for _, c := range req.Cookies() {
		n, v := c.Name, c.Value
		// log.Printf("DEBUG: query parameter: %s=%s", n, v)
		if s := cleanse(n, v); s != "" {
			if q, exists := vars[n]; exists {
				log.Printf(`WARNING: cookie parameter value "%s" overriding query parameter value "%s" for "%s"`, q, v, n)
			}
			vars[n] = s
			// log.Printf("DEBUG: cookie parameter: %s=%s", n, s)
		}
	}

	// Compute the maximum number of variables so that we can allocate maps and slices.
	max := len(vars)
	order := 0

	// This creates functions for each name that exists in the template so that the names can be
	// replaced with sequential bind-variables, $1, $2, ...
	paramFuncMap := make(template.FuncMap, max)
	// This maintains the mapping of template-variable _values_ seen and the order in which it was seen.
	// This is so that the same value will only be passed once.
	parameterMapOrder := make(map[interface{}]int, max)
	// The ordered list of template-values.
	parameters := make([]interface{}, 0)
	// The maping of names to values that will actually be used in the result.
	// Since this depends on unique _values_, it might not be the same as the `vars` map.
	parameterMap := make(map[string]interface{}, max)
	// This is a factory function that creates a function-closure that keeps track of the parameter ordering.
	// This works through the template package. The template package will call the created function
	// as the name is encountered so the created function will return the properly ordered bind-variable
	// based on how the template is processed.
	paramFunc := func(n string, v interface{}) func() interface{} {
		return func() interface{} {
			current, exists := parameterMapOrder[v]
			if !exists {
				order++
				current = order
				parameters = append(parameters, v)
				parameterMap[n] = v
				parameterMapOrder[v] = order
				// log.Printf("DEBUG: paramFunc: %+v %+v %+v", order, n, v)
			}
			return fmt.Sprintf("$%d", current)
		}
	}

	i := 0
	// The actual bind-variable values passed with the query.
	replacements := make([]interface{}, max)

	normalized := map[string]string{}

	// Normalize all the variables.
	for n, v := range vars {
		normalized[n] = v
		paramFuncMap[n] = paramFunc(n, v)
		replacements[i] = v
		i++
	}

	replaceStatics := func(sql string) string {
		// Replace static template vars.
		for _, match := range re_staticVariable.FindAllStringSubmatch(sql, -1) {
			n := match[1]
			r := match[0]
			if _, exists := vars[n]; exists {
				// log.Printf("DEBUG: matched: %+v : %+v : %+v : %+v", match, n, n[1:], vars)
				sql = strings.Replace(sql, r, vars[n], -1)
			} else {
				// log.Printf("DEBUG: ignoring unprovided static parameter %s (%s).", n, r)
				sql = strings.Replace(sql, r, fmt.Sprintf("/-/%s/-/", n), -1)
			}
		}
		return sql
	}
	sqlText = replaceStatics(sqlText)

	if tmpl, err := template.New("").Funcs(paramFuncMap).Parse(sqlText); err != nil {
		log.Panicf("PANIC: %+v\n\tvars: %+v\n\treplacements: %+v\n\tfunctions: %+v\n\n", err, normalized, replacements, paramFuncMap)
	} else {
		var sqlBuffer bytes.Buffer
		tmpl.Execute(&sqlBuffer, replacements)
		sqlText = string(sqlBuffer.Bytes())
	}

	{
		// Restore static variables for which a value is unprovided.
		for _, match := range re_missingVariable.FindAllStringSubmatch(sqlText, -1) {
			n := match[1]
			r := match[0]
			sqlText = strings.Replace(sqlText, r, fmt.Sprintf("{{.%s}}", n), -1)
		}

	}

	// log.Printf("DEBUG: parameterized: %+v\n\t%+v\n\t%s\n\n", vars, parameters, sqlText)

	return sqlText, parameters
}
