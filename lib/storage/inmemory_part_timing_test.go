package storage

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func BenchmarkInmemoryPartInitFromRowsWorstCase(b *testing.B) {
	benchmarkInmemoryPartInitFromRows(b, benchRawRowsWorstCase)
}

func BenchmarkInmemoryPartInitFromRowsBestCase(b *testing.B) {
	benchmarkInmemoryPartInitFromRows(b, benchRawRowsBestCase)
}

func benchmarkInmemoryPartInitFromRows(b *testing.B, rows []rawRow) {
	b.ReportAllocs()
	b.SetBytes(int64(len(rows)))
	b.RunParallel(func(pb *testing.PB) {
		var mp inmemoryPart
		for pb.Next() {
			mp.InitFromRows(rows)
		}
	})
}

// Each row belongs to an unique TSID
var benchRawRowsWorstCase = func() []rawRow {
	var rows []rawRow
	var r rawRow
	for i := 0; i < 1e5; i++ {
		r.TSID.MetricID = uint64(i)
		r.Timestamp = rand.Int63()
		r.Value = rand.NormFloat64()
		r.PrecisionBits = uint8(i%64) + 1
		rows = append(rows, r)
	}
	return rows
}()

// All the rows belong to a single TSID, values are zeros, timestamps
// are delimited by const delta.
var benchRawRowsBestCase = func() []rawRow {
	var rows []rawRow
	var r rawRow
	r.PrecisionBits = defaultPrecisionBits
	for i := 0; i < 1e5; i++ {
		r.Timestamp += 30e3
		rows = append(rows, r)
	}
	return rows
}()


// All rows belongs to a single TSID
var benchRawRowsCityTemp = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/city_temperature-fixed.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		row4, err := strconv.Atoi(row[4])
		row5, err := strconv.Atoi(row[5])
		t, err := time.Parse(layout, fmt.Sprintf("%02d/%02d/%s 00:00:00", row4, row5, row[6]))
		if err != nil {
			fmt.Println(err)
		} else {
			if value, err := strconv.ParseFloat(row[7], 64); err == nil {
				r.TSID.MetricID = uint64(1)
				r.Timestamp = t.UnixNano()
				r.Value = value
				r.PrecisionBits = defaultPrecisionBits
				rows = append(rows, r)
			}
		}
	}
	return rows
}()


// All rows belongs to a single TSID
var benchRawRowsBaselTemp = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/basel-temp.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

// All rows belongs to a single TSID
var benchRawRowsBaselWind = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/basel-wind-speed.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()


// All rows belongs to a single TSID
var benchRawRowsBitcoinPrice = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/bitcoin-price-data.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

// All rows belongs to a single TSID
var benchRawRowsBirdMigration = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/bird-migration-data.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

// All rows belongs to a single TSID
var benchRawRowsAirPressure = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/NEON_pressure-air_staPresMean.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

// All rows belongs to a single TSID
var benchRawRowsAirSensor = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/air-sensor-data.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

// All rows belongs to a single TSID
var benchRawRowsDewPointTemp = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/NEON_rel-humidity-buoy-dewTempMean.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

// All rows belongs to a single TSID
var benchRawRowsPM10 = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/NEON_size-dust-particulate-PM10Median.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

// All rows belongs to a single TSID
var benchRawRowsBioTemp = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/NEON_temp-bio-bioTempMean.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

// All rows belongs to a single TSID
var benchRawRowsWindDir = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/NEON_wind-2d_windDirMean.csv.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

// All rows belongs to a single TSID
var benchRawRowsStocksDE = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/Stocks-Germany.txt.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

// All rows belongs to a single TSID
var benchRawRowsStocksUK = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/Stocks-UK.txt.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

// All rows belongs to a single TSID
var benchRawRowsStocksUSA = func() []rawRow {

	layout := "01/02/2006 15:04:05"
	const defaultPrecisionBits = 64

	var rows []rawRow
	var r rawRow
	f, err := os.Open("/home/panagiotis/timeseries/Stocks-USA.txt.gz")
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		fmt.Println(err)
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), ",")
		t, err := time.Parse(layout, fmt.Sprintf("%s %s", row[0], row[1]))
		if err != nil {
			fmt.Println(err)
		}
		if value, err := strconv.ParseFloat(row[2], 64); err == nil {
			r.TSID.MetricID = uint64(1)
			r.Timestamp = t.UnixNano()
			r.Value = value
			r.PrecisionBits = defaultPrecisionBits
			rows = append(rows, r)
		}
	}
	return rows
}()

