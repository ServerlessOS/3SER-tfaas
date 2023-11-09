package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
)

var nums = flag.Int("nums", 1000, "The total number of experiments in the benchmark")
var numRequests = flag.Int("numRequests", 1000, "The total number of requests in the benchmark")
var numThreads = flag.Int("numThreads", 10, "The total number of parallel threads in the benchmark")
var interval = flag.Int("interval", 0, "")

// ws: write skew, ul: update lost, tiwc: transaction inversion within client
var anomaly = flag.String("anomalyType", "", "The total number of parallel threads in the benchmark")
var TFaaS_url = flag.String("url", "localhost", "The ip address of TFaaS gateway")

var client = &http.Client{}
var coursePercent = []int{80, 98, 99}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	url := fmt.Sprintf("http://%s:8080/function/", *TFaaS_url)
	TFaaS_url = &url
	if *anomaly != "" {
		benchmarkCA()
	} else {
		benchmarkLA()
	}
}

func benchmarkCA() {
	latencyChannel := make(chan []float64)
	errorChannel := make(chan []string)
	abortsChannel := make(chan int)
	latencies := []float64{}
	errors := []string{}
	aborts := 0
	anomalies := 0
	for i := 0; i < *nums; i++ {
		*numThreads = 1
		go func() {
			if *anomaly == "ws" {
				if benchmarkTFaaSWS(latencyChannel, errorChannel, abortsChannel) {
					anomalies++
				}
			} else if *anomaly == "ul" {
				if benchmarkTFaaSUL(latencyChannel, errorChannel, abortsChannel) {
					anomalies++
				}
			} else if *anomaly == "tiwc" {
				if benchmarkTFaaSTIWC(latencyChannel, errorChannel, abortsChannel) {
					anomalies++
				}
			} else {
				log.Println("anomaly error")
			}
		}()
		for i := 0; i < *numThreads; i++ {
			latencyArray := <-latencyChannel
			latencies = append(latencies, latencyArray...)

			errorArray := <-errorChannel
			errors = append(errors, errorArray...)

			aborts += <-abortsChannel
		}
	}

	median, _ := stats.Median(latencies)
	fifth, _ := stats.Percentile(latencies, 5.0)
	nfifth, _ := stats.Percentile(latencies, 95.0)
	first, _ := stats.Percentile(latencies, 1.0)
	nninth, _ := stats.Percentile(latencies, 99.0)

	if len(errors) > 0 {
		fmt.Printf("Errors: %v\n", errors)
	}

	fmt.Printf("Number of errors: %d\n", len(errors))
	fmt.Printf("Median latency: %.6fs\n", median)
	fmt.Printf("5th percentile/95th percentile latency: %.6fs, %.6fs\n", fifth, nfifth)
	fmt.Printf("1st percentile/99th percentile latency: %.6fs, %.6fs\n", first, nninth)
	fmt.Printf("Total aborts: %d\n", aborts)
	fmt.Printf("Total anomalies: %d\n", anomalies)
}

func benchmarkLA() {
	latencyChannel1 := make(chan []float64)
	errorChannel1 := make(chan []string)
	abortsChannel1 := make(chan int)
	latencies1 := []float64{}
	errors1 := []string{}
	aborts1 := 0

	latencyChannel2 := make(chan []float64)
	errorChannel2 := make(chan []string)
	abortsChannel2 := make(chan int)
	latencies2 := []float64{}
	errors2 := []string{}
	aborts2 := 0

	latencyChannel3 := make(chan []float64)
	errorChannel3 := make(chan []string)
	abortsChannel3 := make(chan int)
	latencies3 := []float64{}
	errors3 := []string{}
	aborts3 := 0

	requestsPerThread := *numRequests / *numThreads

	for i := 0; i < *nums; i++ {
		resetInput := map[string]any{"client": "client"}
		resetParams := buildParams("resetworkflow", resetInput)
		_, err := callFunction(*TFaaS_url+"resetworkflow", resetParams)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second)
		for i := 0; i < *numThreads; i++ {
			go func(clientlabel int) {
				l1 := []float64{}
				e1 := []string{}
				a1 := 0
				l2 := []float64{}
				e2 := []string{}
				a2 := 0
				l3 := []float64{}
				e3 := []string{}
				a3 := 0
				for i := 0; i < requestsPerThread; i++ {
					input := map[string]any{"client": "client-" + strconv.Itoa(clientlabel)}
					k := rand.Intn(100)
					funcName := "listCoursesworkflow"
					if k < coursePercent[0] {
						funcName = "listCoursesworkflow"
					} else if k < coursePercent[1] {
						funcName = "registerworkflow"
					} else {
						funcName = "deleteworkflow"
					}
					params := buildParams(funcName, input)

					time.Sleep(4 * time.Millisecond)

					t0 := time.Now()
					resp, err := callFunction(*TFaaS_url+funcName, params)
					t1 := time.Since(t0)
					latency := t1.Seconds()

					if err != nil {
						if funcName == "listCoursesworkflow" {
							e1 = append(e1, err.Error())
						} else if funcName == "registerworkflow" {
							e2 = append(e2, err.Error())
						} else {
							e3 = append(e3, err.Error())
						}

					} else {
						out := map[string]any{}
						json.Unmarshal(resp, &out)
						errString, ok := out["error"]
						if ok {
							if errString.(string) == "abort" {
								if funcName == "listCoursesworkflow" {
									a1++
								} else if funcName == "registerworkflow" {
									a2++
								} else {
									a3++
								}
							} else {
								if funcName == "listCoursesworkflow" {
									e1 = append(e1, err.Error())
								} else if funcName == "registerworkflow" {
									e2 = append(e2, err.Error())
								} else {
									e3 = append(e3, err.Error())
								}
							}
						} else {
							if funcName == "listCoursesworkflow" {
								l1 = append(l1, latency)
							} else if funcName == "registerworkflow" {
								l2 = append(l2, latency)
							} else {
								l3 = append(l3, latency)
							}
						}
					}
				}

				latencyChannel1 <- l1
				latencyChannel2 <- l2
				latencyChannel3 <- l3
				errorChannel1 <- e1
				errorChannel2 <- e2
				errorChannel3 <- e3
				abortsChannel1 <- a1
				abortsChannel2 <- a2
				abortsChannel3 <- a3
			}(i)
		}
		for i := 0; i < *numThreads; i++ {
			latencies1 = append(latencies1, <-latencyChannel1...)
			latencies2 = append(latencies2, <-latencyChannel2...)
			latencies3 = append(latencies3, <-latencyChannel3...)

			errors1 = append(errors1, <-errorChannel1...)
			errors2 = append(errors2, <-errorChannel2...)
			errors3 = append(errors3, <-errorChannel3...)

			aborts1 += <-abortsChannel1
			aborts2 += <-abortsChannel2
			aborts3 += <-abortsChannel3
		}
	}

	fmt.Printf("listCoursesworkflow success: %d\n", len(latencies1))
	median, _ := stats.Median(latencies1)
	fifth, _ := stats.Percentile(latencies1, 5.0)
	nfifth, _ := stats.Percentile(latencies1, 95.0)
	first, _ := stats.Percentile(latencies1, 1.0)
	nninth, _ := stats.Percentile(latencies1, 99.0)
	fmt.Printf("Number of errors: %d\n", len(errors1))
	fmt.Printf("Median latency: %.6fs\n", median)
	fmt.Printf("5th percentile/95th percentile latency: %.6fs, %.6fs\n", fifth, nfifth)
	fmt.Printf("1st percentile/99th percentile latency: %.6fs, %.6fs\n", first, nninth)
	fmt.Printf("Total aborts: %d\n", aborts1)

	fmt.Println("")

	fmt.Printf("registerworkflow success: %d\n", len(latencies2))
	median, _ = stats.Median(latencies2)
	fifth, _ = stats.Percentile(latencies2, 5.0)
	nfifth, _ = stats.Percentile(latencies2, 95.0)
	first, _ = stats.Percentile(latencies2, 1.0)
	nninth, _ = stats.Percentile(latencies2, 99.0)
	fmt.Printf("Number of errors: %d\n", len(errors2))
	fmt.Printf("Median latency: %.6fs\n", median)
	fmt.Printf("5th percentile/95th percentile latency: %.6fs, %.6fs\n", fifth, nfifth)
	fmt.Printf("1st percentile/99th percentile latency: %.6fs, %.6fs\n", first, nninth)
	fmt.Printf("Total aborts: %d\n", aborts2)

	fmt.Println("")

	fmt.Printf("deleteworkflow success: %d\n", len(latencies3))
	median, _ = stats.Median(latencies3)
	fifth, _ = stats.Percentile(latencies3, 5.0)
	nfifth, _ = stats.Percentile(latencies3, 95.0)
	first, _ = stats.Percentile(latencies3, 1.0)
	nninth, _ = stats.Percentile(latencies3, 99.0)
	fmt.Printf("Number of errors: %d\n", len(errors3))
	fmt.Printf("Median latency: %.6fs\n", median)
	fmt.Printf("5th percentile/95th percentile latency: %.6fs, %.6fs\n", fifth, nfifth)
	fmt.Printf("1st percentile/99th percentile latency: %.6fs, %.6fs\n", first, nninth)
	fmt.Printf("Total aborts: %d\n", aborts3)
}

func benchmarkTFaaSWS(latencyChannel chan []float64, errorChannel chan []string, abortsChannel chan int) bool {
	time.Sleep(100 * time.Millisecond)
	resetInput := map[string]any{"client": "client-0"}
	resetParams := buildParams("resetworkflow", resetInput)
	_, err := callFunction(*TFaaS_url+"resetworkflow", resetParams)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	registerInput := map[string]any{"client": "client-1", "sid": "0", "cid": "1000"}
	registerParams, err := json.Marshal(registerInput)
	if err != nil {
		log.Fatal(err)
	}
	deleteInput := map[string]any{"client": "client-2", "cid": "1000"}
	deleteParams, err := json.Marshal(deleteInput)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	aborts := 0
	errors := []string{}
	r := false
	d := false
	t1 := 0.0
	t2 := 0.0

	go func() {
		start := time.Now()
		resp, err := callFunction(*TFaaS_url+"registerworkflow", registerParams)
		t1 = time.Since(start).Seconds()
		if err != nil {
			errors = append(errors, err.Error())
		} else {
			out := map[string]any{}
			json.Unmarshal(resp, &out)
			errString, ok := out["error"]
			if ok {
				if errString.(string) == "abort" {
					log.Println("abort in r")
					aborts++
				} else {
					log.Fatal(errString)
				}
			} else if out["success"].(bool) {
				_, ok := out["message"]
				if !ok {
					r = true
				}
			}
		}
		wg.Done()
	}()
	time.Sleep(time.Duration(*interval) * time.Millisecond)
	go func() {
		start := time.Now()
		resp, err := callFunction(*TFaaS_url+"deleteworkflow", deleteParams)
		t2 = time.Since(start).Seconds()
		if err != nil {
			errors = append(errors, err.Error())
		} else {
			out := map[string]any{}
			json.Unmarshal(resp, &out)
			errString, ok := out["error"]
			if ok {
				if errString.(string) == "abort" {
					log.Println("abort in d")
					aborts++
				} else {
					log.Fatal(errString)
				}
			} else if out["success"].(bool) {
				_, ok := out["message"]
				if !ok {
					d = true
				}
			}
		}

		wg.Done()
	}()
	wg.Wait()
	totalTime := t1 + t2
	latencyChannel <- []float64{totalTime}
	errorChannel <- errors
	if aborts > 0 {
		aborts = 1
	}
	abortsChannel <- aborts
	if r && d {
		return true
	}
	return false
}

func benchmarkTFaaSUL(latencyChannel chan []float64, errorChannel chan []string, abortsChannel chan int) bool {
	time.Sleep(100 * time.Millisecond)
	resetInput := map[string]any{"client": "client-0"}
	resetParams := buildParams("resetworkflow", resetInput)
	_, err := callFunction(*TFaaS_url+"resetworkflow", resetParams)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	registerInput := map[string]any{"client": "client-1", "cid": "1000"}
	for i := 0; i < 19; i++ {
		registerInput["sid"] = strconv.Itoa(i)
		registerParams, err := json.Marshal(registerInput)
		if err != nil {
			log.Fatal(err)
		}
		resp, err := callFunction(*TFaaS_url+"registerworkflow", registerParams)
		if err != nil {
			log.Fatal(err)
		}
		out := map[string]any{}
		json.Unmarshal(resp, &out)
		errString, ok := out["error"]
		if ok {
			if errString.(string) == "abort" {
				log.Printf("abort in i=%d", i)
				i--
			} else {
				log.Fatal(errString)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	registerInput1 := map[string]any{"client": "client-2", "sid": "19", "cid": "1000"}
	registerParams1, err := json.Marshal(registerInput1)
	if err != nil {
		log.Fatal(err)
	}

	registerInput2 := map[string]any{"client": "client-3", "sid": "20", "cid": "1000"}
	registerParams2, err := json.Marshal(registerInput2)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(2)
	aborts := 0
	errors := []string{}
	r1 := false
	r2 := false
	start := time.Now()
	go func() {
		resp, err := callFunction(*TFaaS_url+"registerworkflow", registerParams1)
		if err != nil {
			errors = append(errors, err.Error())
		} else {
			out := map[string]any{}
			json.Unmarshal(resp, &out)
			errString, ok := out["error"]
			if ok {
				if errString.(string) == "abort" {
					log.Println("abort in r1")
					aborts++
				} else {
					log.Fatal(errString)
				}
			} else if out["success"].(bool) {
				_, ok := out["message"]
				if !ok {
					r1 = true
				}
			}
		}
		wg.Done()
	}()
	time.Sleep(10 * time.Millisecond)
	go func() {
		resp, err := callFunction(*TFaaS_url+"registerworkflow", registerParams2)
		if err != nil {
			if err.Error() == "abort" {
				aborts++
			} else {
				errors = append(errors, err.Error())
			}
		} else {
			out := map[string]any{}
			json.Unmarshal(resp, &out)
			errString, ok := out["error"]
			if ok {
				if errString.(string) == "abort" {
					log.Println("abort in r2")
					aborts++
				} else {
					log.Fatal(errString)
				}
			} else if out["success"].(bool) {
				_, ok := out["message"]
				if !ok {
					r2 = true
				}
			}
		}
		wg.Done()
	}()
	wg.Wait()
	totalTime := time.Since(start).Seconds()

	latencyChannel <- []float64{totalTime}
	errorChannel <- errors
	if aborts > 0 {
		aborts = 1
	}
	abortsChannel <- aborts
	if r1 && r2 {
		return true
	}
	return false
}

func benchmarkTFaaSTIWC(latencyChannel chan []float64, errorChannel chan []string, abortsChannel chan int) bool {
	time.Sleep(100 * time.Millisecond)
	resetInput := map[string]any{"client": "client-0"}
	resetParams := buildParams("resetworkflow", resetInput)
	_, err := callFunction(*TFaaS_url+"resetworkflow", resetParams)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)

	registerInput := map[string]any{"client": "client-1", "sid": "0", "cid": "1000"}
	registerParams, err := json.Marshal(registerInput)
	if err != nil {
		log.Fatal(err)
	}
	listCoursesInput := map[string]any{"client": "client-1", "sid": "0"}
	listCoursesParams, err := json.Marshal(listCoursesInput)
	if err != nil {
		log.Fatal(err)
	}

	aborts := 0
	errors := []string{}
	r := false
	cl := []string{}
	start := time.Now()
	resp, err := callFunction(*TFaaS_url+"registerworkflow", registerParams)
	if err != nil {
		errors = append(errors, err.Error())
	} else {
		out := map[string]any{}
		json.Unmarshal(resp, &out)
		errString, ok := out["error"]
		if ok {
			if errString.(string) == "abort" {
				log.Println("abort in r")
				aborts++
			} else {
				log.Fatal(errString)
			}
		} else if out["success"].(bool) {
			_, ok := out["message"]
			if !ok {
				r = true
			}
		}
	}
	resp, err = callFunction(*TFaaS_url+"listCoursesworkflow", listCoursesParams)
	totalTime := time.Since(start).Seconds()
	if err != nil {
		errors = append(errors, err.Error())
	} else {
		out := map[string]any{}
		json.Unmarshal(resp, &out)
		errString, ok := out["error"]
		if ok {
			if errString.(string) == "abort" {
				log.Println("abort in r1")
				aborts++
			} else {
				log.Fatal(errString)
			}
		} else {
			rs := out["courses"]
			for _, ss := range rs.([]any) {
				cl = append(cl, ss.(string))
			}
		}
	}

	latencyChannel <- []float64{totalTime}
	errorChannel <- errors
	if aborts > 0 {
		aborts = 1
	}
	abortsChannel <- aborts
	if r && len(cl) == 0 {
		return true
	}
	return false
}

func callFunction(functionURL string, jsonParams []byte) ([]byte, error) {
	req, err := http.NewRequest("POST", functionURL, bytes.NewBuffer(jsonParams))
	if err != nil {
		log.Println(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	bodyBytes, bodyErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if bodyErr != nil {
		log.Printf("Error reading body from request.")
	}
	return bodyBytes, err
}

func buildParams(funcName string, params map[string]any) []byte {
	if funcName == "deleteworkflow" {
		num := rand.Intn(50) + 1000
		params["cid"] = strconv.Itoa(num)
	} else if funcName == "registerworkflow" {
		num := rand.Intn(50) + 1000
		params["cid"] = strconv.Itoa(num)
		params["sid"] = strconv.Itoa(rand.Intn(50))
	} else if funcName == "listCoursesworkflow" {
		params["sid"] = strconv.Itoa(rand.Intn(50))
	}
	bout, err := json.Marshal(params)
	if err != nil {
		log.Println(err)
		return nil
	}
	return bout
}
