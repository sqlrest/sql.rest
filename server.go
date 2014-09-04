package main

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"runtime"
	"text/template"
	"time"
)

//
func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

//
func Select(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Caught panic: %v\nrequest: %v", r, req)
			http.Error(w, fmt.Sprintf("%v", r), http.StatusInternalServerError)
		}
	}()
	if sqlText, err := ioutil.ReadAll(req.Body); err != nil {
		log.Panic(err)
	} else {
		if db, err := sql.Open("postgres", "sslmode=disable connect_timeout=10"); err != nil {
			log.Panic(err)
		} else {
			values := req.URL.Query()
			cookies := req.Cookies()

			max := len(values) + len(cookies)
			order := 0

			paramFuncMap := make(template.FuncMap, max)
			parameterMapOrder := make(map[interface{}]int, max)
			parameters := make([]interface{}, 0)
			parameterMap := make(map[string]interface{}, max)
			paramFunc := func(n string, v interface{}) func() interface{} {
				return func() interface{} {
					current, exists := parameterMapOrder[v]
					if !exists {
						order++
						current = order
						parameters = append(parameters, v)
						parameterMap[n] = v
						parameterMapOrder[v] = order
						log.Printf("\nparamFunc: %+v %+v %+v", order, n, v)
					}
					return fmt.Sprintf("$%d", current)
				}
			}

			i := 0
			replacements := make([]interface{}, max)

			for n, v := range values {
				if len(v) > 0 {
					paramFuncMap[n] = paramFunc(n, v[0])
					replacements[i] = v[0]
					i++
				}
			}

			for _, c := range cookies {
				n := c.Name
				v := c.Value
				if _, exists := paramFuncMap[n]; !exists {
					paramFuncMap[n] = paramFunc(n, v)
					replacements[i] = v
					i++
				}
			}

			if tmpl, err := template.New("").Funcs(paramFuncMap).Parse(string(sqlText)); err != nil {
				log.Panic(err)
			} else {
				var sqlBuffer bytes.Buffer
				tmpl.Execute(&sqlBuffer, replacements)
				sqlText = sqlBuffer.Bytes()
			}

			{
				nows := fmt.Sprintf("%8x", time.Now().UTC().UnixNano())
				rnds := fmt.Sprintf("%8x", rand.Int63())
				requestId := fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%v.%v", nows, rnds))))
				statementId := fmt.Sprintf("%x", md5.Sum([]byte(sqlText)))
				parameterSum := md5.New()
				for _, p := range parameters {
					pv := fmt.Sprintf("%+v", p)
					log.Printf("summing parameter value [%v]", pv)
					parameterSum.Write([]byte(pv))
				}
				parameterId := fmt.Sprintf("%x", parameterSum.Sum(nil))
				queryId := fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%x%x", statementId, parameterId))))

				for n, v := range parameterMap {
					http.SetCookie(w, &http.Cookie{Name: n, Value: fmt.Sprintf("%+v", v)})
				}

				h := w.Header()
				h.Set("Content-Type", "text/tab-separated-values; charset=utf-8")
				h.Set("X-Powered-By", "sql.rest")
				h.Set("X-Request-Id", requestId)
				h.Set("X-SQL-Statement-Id", statementId)
				h.Set("X-SQL-Query-Id", queryId)
				h.Set("X-SQL-Parameters-Id", parameterId)
				h.Add("Access-Control-Allow-Origin", "*")

				if true {
					h.Set("X-SQL-Statement", string(sqlText))
					h.Set("X-SQL-Query", fmt.Sprintf("%s %+v", sqlText, parameters))
					h.Set("X-SQL-Parameters", fmt.Sprintf("%+v", parameters))
				}
			}

			if rows, err := db.Query(string(sqlText), parameters...); err != nil {
				log.Panic(err)
			} else {
				defer rows.Close()
				if cols, err := rows.Columns(); err != nil {
					log.Panic(err)
				} else {

					count := len(cols)
					for i, c := range cols {
						if i != 0 {
							fmt.Fprintf(w, "\t")
						}
						fmt.Fprintf(w, "%v", c)
					}
					fmt.Fprintf(w, "\n")

					values := make([]interface{}, count)
					valuePtrs := make([]interface{}, count)

					for rows.Next() {
						for i, _ := range cols {
							valuePtrs[i] = &values[i]
						}
						err := rows.Scan(valuePtrs...)
						if err != nil {
							log.Panic(err)
						}
						for i, _ := range cols {
							var v interface{}
							val := values[i]
							b, ok := val.([]byte)
							if ok {
								v = string(b)
							} else {
								v = val
							}
							if i != 0 {
								fmt.Fprintf(w, "\t")
							}
							if v != nil {
								fmt.Fprintf(w, "%v", v)
							}
						}
						fmt.Fprintf(w, "\n")
					}
				}
			}
		}
	}
}

//
func Test(w http.ResponseWriter, req *http.Request) {
	if body, err := ioutil.ReadAll(req.Body); err != nil {
		log.Panic(err)
	} else {

		values := req.URL.Query()
		paramFuncMap := make(template.FuncMap, len(values))
		order := 0
		parameterMapOrder := make(map[interface{}]int, len(values))
		parameters := make([]interface{}, 0)
		paramFunc := func(n string, v interface{}) func() interface{} {
			return func() interface{} {
				current, exists := parameterMapOrder[v]
				if !exists {
					order++
					current = order
					parameters = append(parameters, v)
					parameterMapOrder[v] = order
					log.Printf("\nparamFunc: %+v %+v %+v", order, n, v)
				}
				return fmt.Sprintf("$%d", current)
			}
		}
		i := 0
		replacements := make([]interface{}, len(values))
		for n, v := range values {
			if len(v) > 0 {
				paramFuncMap[n] = paramFunc(n, v[0])
				replacements[i] = v[0]
				i++
			}
		}

		log.Printf("paramFuncMap: %v", paramFuncMap)
		if tmpl, err := template.New("").Funcs(paramFuncMap).Parse(string(body)); err != nil {
			log.Panic(err)
		} else {
			tmpl.Execute(w, replacements)
			fmt.Fprintf(w, "parameters %+v", parameters)
		}
	}
}

//
func Usage(w http.ResponseWriter, req *http.Request) {
	if body, err := ioutil.ReadAll(req.Body); err != nil {
		log.Panic(err)
	} else {
		fmt.Fprintf(w, "%v\n", string(body))
	}
}

//
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	addr := "localhost:3030"
	log.Printf("Listening on %s", addr)

	r := mux.NewRouter()
	r.HandleFunc("/test", Test)
	r.HandleFunc("/sql", Select)
	r.HandleFunc("/", Usage)
	http.Handle("/", r)

	log.Fatal(http.ListenAndServe(addr, r))

	// log.Fatal("done")
}
