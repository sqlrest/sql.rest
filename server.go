package main

import (
	"bytes"
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"time"

	"github.com/didip/tollbooth"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/pmylund/go-cache"
)

//
func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

//
func timer(key, name string, startLog ...bool) func() {
	t := time.Now()
	if len(startLog) > 0 && startLog[0] {
		log.Printf("%s Start %+v: %+v\n", key, t, name)
	}
	return func() {
		log.Printf("%s Elapsed %6.4fs : %+v\n", key, time.Since(t).Seconds(), name)
	}
}

var (
	// Create a cache with a default expiration time of 5 minutes, and which purges expired items every 30 seconds.
	_cache       = cache.New(5*time.Minute, 30*time.Second)
	_usage       = cache.New(24*time.Hour, 6*time.Hour)
	_limiter     = tollbooth.NewLimiter(3, time.Second)
	re_normalize = regexp.MustCompile(`\s+`)
	s_normalize  = []byte(" ")
)

//
func SQL(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Caught panic: %v\nrequest: %v", r, req)
			http.Error(w, fmt.Sprintf("%v", r), http.StatusInternalServerError)
		}
	}()
	switch method := req.Method; method {
	case "POST":
		Create(w, req)
	case "GET":
		Read(w, req)
	case "PUT":
		CreateOrUpdate(w, req)
	case "PATCH":
		Update(w, req)
	case "DELETE":
		Delete(w, req)
	default:
		log.Printf("Method: %v", method)
	}
}

//
func Create(w http.ResponseWriter, req *http.Request) {
	http.Error(w, fmt.Sprintf("Create: %v", req.Method), http.StatusNotImplemented)
}

//
func CreateOrUpdate(w http.ResponseWriter, req *http.Request) {
	http.Error(w, fmt.Sprintf("Create or Update: %v", req.Method), http.StatusNotImplemented)
}

//
func Update(w http.ResponseWriter, req *http.Request) {
	http.Error(w, fmt.Sprintf("Update: %v", req.Method), http.StatusNotImplemented)
}

//
func Delete(w http.ResponseWriter, req *http.Request) {
	http.Error(w, fmt.Sprintf("Delete: %v", req.Method), http.StatusNotImplemented)
}

//
func Read(w http.ResponseWriter, req *http.Request) {
	// defer timer(req.URL.String(), "read")()

	if sqlText, err := ioutil.ReadAll(req.Body); err != nil {
		log.Panic(err)
	} else {
		sqlText = re_normalize.ReplaceAll(sqlText, s_normalize)

		var parameters []interface{}
		{
			q, p := parameterize(w, req, string(sqlText))
			sqlText, parameters = []byte(q), p
		}

		queryId := ""
		func() {
			// defer timer(req.URL.String(), "header")()

			nows := fmt.Sprintf("%8x", time.Now().UTC().UnixNano())
			rnds := fmt.Sprintf("%8x", rand.Int63())
			requestName := fmt.Sprintf("%v.%v", nows, rnds)
			requestId := fmt.Sprintf("%x", md5.Sum([]byte(requestName)))
			statementId := fmt.Sprintf("%x", md5.Sum(sqlText))
			parametersName := fmt.Sprintf("%+v", parameters)
			parameterId := fmt.Sprintf("%x", md5.Sum([]byte(parametersName)))
			queryName := fmt.Sprintf("%s %+v", sqlText, parameters)
			queryId = fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprintf("%x%x", statementId, parameterId))))

			h := w.Header()
			h.Set("Content-Type", "text/tab-separated-values; charset=utf-8")
			h.Set("X-Powered-By", "sql.rest")
			h.Set("X-Request-Id", requestId)
			h.Set("X-SQL-Statement-Id", statementId)
			h.Set("X-SQL-Query-Id", queryId)
			h.Set("X-SQL-Parameters-Id", parameterId)
			h.Add("Access-Control-Allow-Origin", "*")

			if req.Header.Get("debug") != "" {
				h.Set("X-SQL-Statement", string(sqlText))
				h.Set("X-SQL-Query", queryName)
				h.Set("X-SQL-Parameters", parametersName)
			}

		}()

		response := ""
		if q, f := _cache.Get(queryId); f {

			if err := _usage.Increment(queryId, 1); err != nil {
				log.Printf("WARNING: Can not increment usage for %s", queryId)
			}
			log.Printf("Hit: %s", queryId)
			response = q.(string)

		} else {

			if httpError := tollbooth.LimitByRequest(_limiter, req); httpError != nil {
				http.Error(w, httpError.Message, httpError.StatusCode)
				return
			}

			_usage.Set(queryId, 1, cache.DefaultExpiration)

			func() {
				defer timer(queryId, "query")()

				log.Printf("Miss: %s", queryId)

				// Allow for custom formatting of different types.
				var formatsParameters = map[string]reflect.Type{
					"_float": reflect.TypeOf(float64(1.0)),
					"_int":   reflect.TypeOf(int64(1)),
				}
				var formats = map[reflect.Type]string{
					formatsParameters["_float"]: "g",
					formatsParameters["_int"]:   "d",
				}
				for n, v := range req.URL.Query() {
					if format, exists := formatsParameters[n]; exists {
						log.Printf("formatting %v value %v", n, v[0])
						formats[format] = v[0]
					}
				}

				buffer := bytes.Buffer{}
				tooBig := false
				rowCount := 0

				if db, err := sql.Open("postgres", "sslmode=disable connect_timeout=10"); err != nil {
					log.Panic(err)
				} else {

					t := time.Now()

					if rows, err := db.Query(string(sqlText), parameters...); err != nil {
						log.Panic(err)
					} else {
						defer rows.Close()
						if cols, err := rows.Columns(); err != nil {
							log.Panic(err)
						} else {

							colCount := len(cols)
							for i, c := range cols {
								if i != 0 {
									fmt.Fprintf(&buffer, "\t")
								}
								fmt.Fprintf(&buffer, "%v", c)
							}
							fmt.Fprintf(&buffer, "\n")

							values := make([]interface{}, colCount)
							valuePtrs := make([]interface{}, colCount)

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
										fmt.Fprintf(&buffer, "\t")
									}
									if v != nil {
										switch v.(type) {
										case float64, int64:
											format, ok := formats[reflect.ValueOf(v).Type()]
											if !ok {
												format = "v"
											}
											fmt.Fprintf(&buffer, fmt.Sprintf("%%%s", format), v)
										default:
											fmt.Fprintf(&buffer, "%v", v)
										}
									}
								}
								fmt.Fprintf(&buffer, "\n")
								rowCount++
							}
						}

						response = buffer.String()
						if !tooBig {
							cost := 1 * time.Hour * time.Duration(int64(time.Since(t).Seconds()))
							log.Printf("%s expiry: %+v", queryId, cost)
							go _cache.Set(queryId, response, cost)
						}
					}
				}
			}()
		}
		fmt.Fprintf(w, response)
	}
}

//
func Test(w http.ResponseWriter, req *http.Request) {
	if sqlText, err := ioutil.ReadAll(req.Body); err != nil {
		log.Panic(err)
	} else {
		sqlText = re_normalize.ReplaceAll(sqlText, s_normalize)

		sql, parameters := parameterize(w, req, string(sqlText))
		fmt.Fprintf(w, "%s %+v\n", sql, parameters)
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
	r.HandleFunc("/sql", SQL)
	r.HandleFunc("/", Usage)
	http.Handle("/", r)

	log.Fatal(http.ListenAndServe(addr, r))

	// log.Fatal("done")
}
